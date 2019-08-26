package com.basic.rdma.task;

import com.basic.rdma.Constants;
import com.basic.rdma.util.CmdLineCommon;
import com.basic.rdmachannel.channel.RdmaChannel;
import com.basic.rdmachannel.channel.RdmaCompletionListener;
import com.basic.rdmachannel.mr.RdmaBuffer;
import com.basic.rdmachannel.mr.RdmaBufferManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

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
        CyclicBarrier cyclicBarrier=new CyclicBarrier(2);

        File file= new File(filePath);
        if(file.exists())
            file.delete();
        RandomAccessFile randomAccessFile=new RandomAccessFile(file, "rw");
        FileChannel fileChannel = randomAccessFile.getChannel();

        // data index transferSize
        RdmaBuffer dataBuffer = rdmaBufferManager.get(cmdLineCommon.getSize()+ Constants.BLOCKINDEX_SIZE + Constants.BLOCKLENGTH_SIZE);
        RdmaBuffer infoBuffer = rdmaBufferManager.get(Constants.INFOBUFFER_SIZE);
        ByteBuffer dataByteBuffer = dataBuffer.getByteBuffer();
        ByteBuffer infoByteBuffer = infoBuffer.getByteBuffer();

        int splitSize=0;
        long fileLength=0L;

        rdmaChannel.rdmaReceiveInQueue(new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buf, Integer IMM) {
                logger.info("infoBuffer RECEIVE Success!!!");
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
        },infoBuffer.getAddress(),infoBuffer.getLength(),infoBuffer.getLkey());
        cyclicBarrier.await();

        splitSize = infoByteBuffer.getInt();
        fileLength = infoByteBuffer.getLong();
        logger.info("Transfer Split File {} Block , Filelength {}", splitSize, fileLength);
        rdmaBufferManager.put(infoBuffer);

        for (int i = 0; i < splitSize; i++) {
            cyclicBarrier.reset();
            rdmaChannel.rdmaReceiveInQueue(new RdmaCompletionListener() {
                @Override
                public void onSuccess(ByteBuffer buf, Integer IMM) {
                    try {
                        dataByteBuffer.clear();
                        int index = dataByteBuffer.getInt();
                        long size = dataByteBuffer.getLong();

                        logger.info("BLOCK {} RECEIVE Success!!! : {}", index, size);
                        dataByteBuffer.limit((int) (size + Constants.BLOCKINDEX_SIZE+ Constants.BLOCKLENGTH_SIZE));
                        while(dataByteBuffer.hasRemaining()){
                            fileChannel.write(dataByteBuffer);
                        }
                        cyclicBarrier.await();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void onFailure(Throwable exception) {
                    exception.printStackTrace();
                }
            },dataBuffer.getAddress(),dataBuffer.getLength(),dataBuffer.getLkey());
            cyclicBarrier.await();
        }

        fileChannel.close();
        rdmaBufferManager.put(dataBuffer);
    }
}
