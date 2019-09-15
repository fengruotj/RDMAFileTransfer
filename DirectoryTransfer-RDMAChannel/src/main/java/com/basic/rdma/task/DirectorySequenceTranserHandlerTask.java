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
public class DirectorySequenceTranserHandlerTask implements Runnable{
    private static final Logger logger = LoggerFactory.getLogger(DirectorySequenceTranserHandlerTask.class);
    private RdmaChannel rdmaChannel;
    private RdmaBufferManager rdmaBufferManager;
    private CmdLineCommon cmdLineCommon;


    public DirectorySequenceTranserHandlerTask(CmdLineCommon cmdLineCommon, RdmaChannel rdmaChannel, RdmaBufferManager rdmaBufferManager) {
        this.rdmaChannel = rdmaChannel;
        this.rdmaBufferManager=rdmaBufferManager;
        this.cmdLineCommon = cmdLineCommon;
    }

    @Override
    public void run() {
        try {
            recvSingleDirectory(cmdLineCommon.getPath());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 接受整个文件夹传输
     * @param directoryPath
     * @throws Exception
     */
    public void recvSingleDirectory(String directoryPath) throws Exception {
        CyclicBarrier cyclicBarrier=new CyclicBarrier(2);

        File file= new File(directoryPath);
        if(file.exists())
            file.delete();

        // data index transferSize
        RdmaBuffer infoBuffer = rdmaBufferManager.get(Constants.INFOBUFFER_SIZE);
        ByteBuffer infoByteBuffer = infoBuffer.getByteBuffer();

        int filesLength=0;
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

        filesLength = infoByteBuffer.getInt();
        logger.info("Transfer directoryPath: {} , file Number: {}", directoryPath, filesLength);
        rdmaBufferManager.put(infoBuffer);

        // Transfer singleFile
        for (int i = 0; i < filesLength; i++) {
            recvSingleFile(directoryPath);
        }
    }

    /**
     * 接受单个文件传输
     * @throws Exception
     */
    public void recvSingleFile(String parentPath) throws Exception {
        CyclicBarrier cyclicBarrier=new CyclicBarrier(2);

        // data index transferSize
        RdmaBuffer dataBuffer = rdmaBufferManager.get(cmdLineCommon.getSize()+ Constants.BLOCKINDEX_SIZE + Constants.BLOCKLENGTH_SIZE);
        RdmaBuffer infoBuffer = rdmaBufferManager.get(Constants.INFOBUFFER_SIZE);
        ByteBuffer dataByteBuffer = dataBuffer.getByteBuffer();
        ByteBuffer infoByteBuffer = infoBuffer.getByteBuffer();

        int splitSize=0;
        long fileLength=0L;
        int fileNameLength=0;
        String fileName= null;
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
        fileNameLength = infoByteBuffer.getInt();
        byte[] data = new byte[fileNameLength];
        infoByteBuffer.get(data);
        fileName = new String(data,"UTF-8");
        logger.info("Transfer FileName {}, Split File {} Block , Filelength {}", fileName, splitSize, fileLength);
        rdmaBufferManager.put(infoBuffer);

        File file= new File(parentPath,fileName);
        if(file.exists())
            file.delete();
        RandomAccessFile randomAccessFile=new RandomAccessFile(file, "rw");
        FileChannel fileChannel = randomAccessFile.getChannel();

        for (int i = 0; i < splitSize; i++) {
            cyclicBarrier.reset();
            rdmaChannel.rdmaReceiveInQueue(new RdmaCompletionListener() {
                @Override
                public void onSuccess(ByteBuffer buf, Integer IMM) {
                    try {
                        dataByteBuffer.clear();
                        int index = dataByteBuffer.getInt();
                        long length = dataByteBuffer.getLong();

                        logger.info("BLOCK {} RECEIVE Success!!! : {}", index, length);
                        dataByteBuffer.limit((int) (length + Constants.BLOCKINDEX_SIZE+ Constants.BLOCKLENGTH_SIZE));
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
