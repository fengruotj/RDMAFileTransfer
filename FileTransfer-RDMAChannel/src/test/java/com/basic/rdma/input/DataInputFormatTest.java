package com.basic.rdma.input;

import org.junit.Test;

import java.io.IOException;

/**
 * locate com.basic.rdma.input
 * Created by master on 2019/8/25.
 */
public class DataInputFormatTest {
    public static final DataInputFormat inputFormat=new DataInputFormat(1024*1024L);

    @Test
    public void getSplits() throws IOException {
        for (InputSplit split : inputFormat.getSplits("../README.md")) {
            System.out.println(split);
        }
    }
}
