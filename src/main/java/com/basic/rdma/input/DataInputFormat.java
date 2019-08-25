package com.basic.rdma.input;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 79875 on 2017/4/1.
 */
public class DataInputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(DataInputFormat.class);

    private Long blockSize=null;
    private static int SPLIT_SLOP = 0;

    public DataInputFormat(Long blockSize) {
        this.blockSize = blockSize;
    }

    public DataInputFormat() {
    }

    public Long getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(Long blockSize) {
        this.blockSize = blockSize;
    }

    public List<InputSplit> getSplits(String inputpath) throws IOException {

        File file=new File(inputpath);
        // splits链表用来存储计算得到的输入分片结果
        List<InputSplit> splits = new ArrayList<InputSplit>();
        Path path = Paths.get(file.getPath());
        long length = file.length();

        // 判断文件是否可分割，通常是可分割的，但如果文件是压缩的，将不可分割
        // 是否分割可以自行重写FileInputFormat的isSplitable来控制
        if ((length != 0)) {
            long bytesRemaining = length;
            // 循环分片。
            // 当剩余数据与分片大小比值大于Split_Slop时，继续分片， 小于等于时，停止分片
            while (bytesRemaining / blockSize > SPLIT_SLOP) {
                splits.add(new FileSplit(path, length - bytesRemaining,
                        blockSize));
                bytesRemaining -= blockSize;
            }
            // 处理余下的数据
            if (bytesRemaining != 0) {
                splits.add(new FileSplit(path, length - bytesRemaining,
                        bytesRemaining));
            }
        } else {
            // 对于长度为0的文件，创建空Hosts列表，返回
            splits.add(new FileSplit(path, 0, length));
        }
        return splits;
    }

    protected FileSplit makeSplit(Path file, long start, long length, String[] hosts, String[] inMemoryHosts) {
        return new FileSplit(file, start, length);
    }
}

