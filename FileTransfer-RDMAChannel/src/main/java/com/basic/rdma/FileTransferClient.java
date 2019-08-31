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
import com.basic.rdmachannel.util.RDMAUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

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
        String hostName = RDMAUtils.getLocalHostLANAddress(cmdLineCommon.getIface()).getHostName();
        this.cmdLineCommon = cmdLineCommon;
        this.rdmaClient=new RdmaNode(hostName, cmdLineCommon.getPort(), rdmaChannelConf , RdmaChannel.RdmaChannelType.RPC);
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

    public void stop() throws Exception {
        this.rdmaClient.stop();
    }

    /**
     * 客户端发送单个文件
     * @param filePath
     * @throws IOException
     * @throws InterruptedException
     */
    public void sendSingleFile(String filePath) throws Exception {
        File file= new File(filePath);
        RandomAccessFile randomAccessFile=new RandomAccessFile(file, "rw");
        List<InputSplit> splits = dataInputFormat.getSplits(filePath);
        // data index transferSize
        RdmaBuffer dataBuffer = rdmaBufferManager.get(cmdLineCommon.getSize()+ Constants.BLOCKINDEX_SIZE + Constants.BLOCKLENGTH_SIZE);
        ByteBuffer dataByteBuffer = dataBuffer.getByteBuffer();
        RdmaBuffer infoBuffer = rdmaBufferManager.get(Constants.INFOBUFFER_SIZE);
        ByteBuffer infoByteBuffer = infoBuffer.getByteBuffer();

        // File Information
        infoByteBuffer.putInt(splits.size());
        infoByteBuffer.putLong(file.length());
        logger.info("Transfer Split File {} Block , Filelength {}", splits.size(), file.length());
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
        CyclicBarrier cyclicBarrier=new CyclicBarrier(2);
        for (int i = 0; i < splits.size(); i++) {
            cyclicBarrier.reset();

            dataByteBuffer.clear();
            InputSplit inputSplit = splits.get(i);
            long length = inputSplit.getLength();
            dataByteBuffer.putInt(i);
            dataByteBuffer.putLong(length);
            randomAccessFile.getChannel().read(dataByteBuffer);

            int finalI = i;
            clientChannel.rdmaSendInQueue(new RdmaCompletionListener() {
                @Override
                public void onSuccess(ByteBuffer buf, Integer IMM) {
                    logger.info("Block {} SEND SUCCESS: {} " , finalI , length);
                    try {
                        cyclicBarrier.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (BrokenBarrierException e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void onFailure(Throwable exception) {
                    exception.printStackTrace();
                }
            },new long[]{dataBuffer.getAddress()},new int[]{dataBuffer.getLength()},new int[]{dataBuffer.getLkey()});
            cyclicBarrier.await();
        }

        rdmaBufferManager.put(dataBuffer);
    }
}
