package com.basic.rdma;

import com.basic.rdma.input.DataInputFormat;
import com.basic.rdma.input.InputSplit;
import com.basic.rdma.util.CmdLineCommon;
import com.basic.rdmachannel.channel.RdmaChannel;
import com.basic.rdmachannel.channel.RdmaChannelConf;
import com.basic.rdmachannel.channel.RdmaCompletionListener;
import com.basic.rdmachannel.channel.RdmaNode;
import com.basic.rdmachannel.mr.RdmaBuffer;
import com.basic.rdmachannel.mr.RdmaBufferManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * locate com.basic.rdma
 * Created by master on 2019/8/25.
 */
public class FileTransferClient {
    private static final Logger logger = LoggerFactory.getLogger(FileTransferClient.class);

    private DataInputFormat dataInputFormat;

    private RdmaChannel clientChannel;
    private RdmaNode rdmaClient;
    private RdmaBufferManager rdmaBufferManager;

    private CmdLineCommon cmdLineCommon;
    public FileTransferClient(CmdLineCommon cmdLineCommon,RdmaChannelConf rdmaChannelConf) throws Exception {
        this.cmdLineCommon = cmdLineCommon;
        this.rdmaClient=new RdmaNode(cmdLineCommon.getIp(), cmdLineCommon.getPort(), rdmaChannelConf , RdmaChannel.RdmaChannelType.RPC);
        this.rdmaBufferManager = rdmaClient.getRdmaBufferManager();
        this.dataInputFormat=new DataInputFormat((long) cmdLineCommon.getSize());
    }

    /**
     * 连接RDMA文件传输服务器
     * @throws IOException
     * @throws InterruptedException
     */
    public void connect(String host, int port) throws IOException, InterruptedException {
        clientChannel = rdmaClient.getRdmaChannel(new InetSocketAddress(host, port), true, RdmaChannel.RdmaChannelType.RPC);
    }

    /**
     * 客户端发送单个文件
     * @param filePath
     * @throws IOException
     * @throws InterruptedException
     */
    public void sendSingleFile(String filePath) throws IOException, InterruptedException {
        File file= new File(filePath);
        RandomAccessFile randomAccessFile=new RandomAccessFile(file, "rw");
        List<InputSplit> splits = dataInputFormat.getSplits(filePath);
        // data index transferSize
        RdmaBuffer dataBuffer = rdmaBufferManager.get(cmdLineCommon.getSize()+ 4 + 8);
        RdmaBuffer infoBuffer = rdmaBufferManager.get(4096);

        // File Information
        infoBuffer.getByteBuffer().putInt(splits.size());
        infoBuffer.getByteBuffer().putLong(file.length());
        clientChannel.rdmaSendInQueue(new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buf, Integer IMM) {
                logger.info("infoBuffer SEND Success!!!");
                rdmaBufferManager.put(infoBuffer);
            }

            @Override
            public void onFailure(Throwable exception) {
                exception.printStackTrace();
                rdmaBufferManager.put(infoBuffer);
            }
        },new long[]{infoBuffer.getAddress()},new int[]{infoBuffer.getLength()},new int[]{infoBuffer.getLkey()});

        // File Data
        for (int i = 0; i < splits.size(); i++) {
            dataBuffer.getByteBuffer().clear();
            InputSplit inputSplit = splits.get(i);
            dataBuffer.getByteBuffer().putInt(i);
            dataBuffer.getByteBuffer().putLong(inputSplit.getLength());
            randomAccessFile.getChannel().read(dataBuffer.getByteBuffer());

            clientChannel.rdmaSendInQueue(new RdmaCompletionListener() {
                @Override
                public void onSuccess(ByteBuffer buf, Integer IMM) {
                    logger.info("Block SEND SUCCESS" );
            }

            @Override
                public void onFailure(Throwable exception) {
                    exception.printStackTrace();
                }
            },new long[]{dataBuffer.getAddress()},new int[]{dataBuffer.getLength()},new int[]{dataBuffer.getLkey()});
        }

        rdmaBufferManager.put(infoBuffer);
    }
}
