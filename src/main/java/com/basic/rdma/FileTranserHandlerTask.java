package com.basic.rdma;

import com.basic.rdma.util.CmdLineCommon;
import com.basic.rdmachannel.channel.RdmaChannel;
import com.basic.rdmachannel.channel.RdmaCompletionListener;
import com.basic.rdmachannel.mr.RdmaBuffer;
import com.basic.rdmachannel.mr.RdmaBufferManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * locate com.basic.rdma
 * Created by master on 2019/8/25.
 */
public class FileTranserHandlerTask implements Runnable{
    private static final Logger logger = LoggerFactory.getLogger(FileTranserHandlerTask.class);
    private RdmaChannel rdmaChannel;
    private RdmaBufferManager rdmaBufferManager;
    private CmdLineCommon cmdLineCommon;

    public FileTranserHandlerTask(CmdLineCommon cmdLineCommon, RdmaChannel rdmaChannel, RdmaBufferManager rdmaBufferManager) {
        this.rdmaChannel = rdmaChannel;
        this.rdmaBufferManager=rdmaBufferManager;
        this.cmdLineCommon = cmdLineCommon;
    }

    @Override
    public void run() {
        try {
            recvSingleFile(cmdLineCommon.getPath());
    } catch (Exception e) {
        e.printStackTrace();
    }
}

    /**
     * 接受单个文件传输
     * @param filePath
     * @throws Exception
     */
    public void recvSingleFile(String filePath) throws Exception {
        File file= new File(filePath);
        RandomAccessFile randomAccessFile=new RandomAccessFile(file, "rw");

        // data index transferSize
        RdmaBuffer dataBuffer = rdmaBufferManager.get(cmdLineCommon.getSize()+ 4 + 8);
        RdmaBuffer infoBuffer = rdmaBufferManager.get(4096);

        int splitSize=0;
        long fileLength=0L;

        rdmaChannel.rdmaReceiveInQueue(new RdmaCompletionListener() {
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
        splitSize = infoBuffer.getByteBuffer().getInt();
        fileLength = infoBuffer.getByteBuffer().getLong();
        logger.info("Transfer Split File {} Block , Filelength {}", splitSize, fileLength);

        for (int i = 0; i < splitSize; i++) {
            rdmaChannel.rdmaReceiveInQueue(new RdmaCompletionListener() {
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
}
