package com.basic.rdma;

import com.basic.rdma.util.CmdLineCommon;
import com.basic.rdmachannel.channel.*;
import com.basic.rdmachannel.mr.RdmaBuffer;
import com.basic.rdmachannel.mr.RdmaBufferManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

/**
 * locate com.basic.rdma
 * Created by master on 2019/8/25.
 */
public class FileTransferServer implements RdmaConnectListener {
    private static final Logger logger = LoggerFactory.getLogger(FileTransferServer.class);
    private CountDownLatch countDownLatch=new CountDownLatch(1);

    private RdmaChannel clientChannel;
    private RdmaNode rdmaServer;
    private RdmaBufferManager rdmaBufferManager;
    private RdmaBuffer rdmaBuffer;

    private CmdLineCommon cmdLineCommon;
    public FileTransferServer(CmdLineCommon cmdLineCommon, RdmaChannelConf rdmaChannelConf) throws Exception {
        this.cmdLineCommon = cmdLineCommon;
        this.rdmaServer=new RdmaNode(cmdLineCommon.getIp(), cmdLineCommon.getPort(), rdmaChannelConf , RdmaChannel.RdmaChannelType.RPC);
        this.rdmaBufferManager = rdmaServer.getRdmaBufferManager();
    }

    public void bind() throws Exception {
        rdmaServer.bindConnectCompleteListener(this);
        countDownLatch.await();
    }

    public void recvSingleFile(String filePath) throws Exception {
        File file= new File(filePath);
        RandomAccessFile randomAccessFile=new RandomAccessFile(file, "rw");

        // data index transferSize
        RdmaBuffer dataBuffer = rdmaBufferManager.get(cmdLineCommon.getSize()+ 4 + 8);
        RdmaBuffer infoBuffer = rdmaBufferManager.get(4096);

        int size=0;
        long fileLength=0L;

        clientChannel.rdmaReceiveInQueue(new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buf, Integer IMM) {
                logger.info("infoBuffer RECEIVE Success!!!");
                rdmaBufferManager.put(infoBuffer);
            }

            @Override
            public void onFailure(Throwable exception) {
                exception.printStackTrace();
                rdmaBufferManager.put(infoBuffer);
            }
        },infoBuffer.getAddress(),infoBuffer.getLength(),infoBuffer.getLkey());
        size = infoBuffer.getByteBuffer().getInt();
        fileLength = infoBuffer.getByteBuffer().getLong();

        for (int i = 0; i < size; i++) {
            clientChannel.rdmaReceiveInQueue(new RdmaCompletionListener() {
                @Override
                public void onSuccess(ByteBuffer buf, Integer IMM) {
                    try {
                        int index = dataBuffer.getByteBuffer().getInt();
                        long size = dataBuffer.getByteBuffer().getLong();
                        logger.info("BLOCK {} RECEIVE Success!!! : {}", index, size);

                        randomAccessFile.write(dataBuffer.getByteBuffer().array(),12, (int) size);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void onFailure(Throwable exception) {
                    exception.printStackTrace();
                }
            },dataBuffer.getAddress(),dataBuffer.getLength(),dataBuffer.getLkey());
        }
        rdmaBufferManager.put(infoBuffer);
    }
    @Override
    public void onSuccess(InetSocketAddress inetSocketAddress, RdmaChannel rdmaChannel) {
        logger.info("success accept RdmaChannel: " +inetSocketAddress.getHostName());
        logger.info(rdmaChannel.toString());
        clientChannel=rdmaChannel;
        countDownLatch.countDown();
    }

    @Override
    public void onFailure(Throwable exception) {
        exception.printStackTrace();
    }
}
