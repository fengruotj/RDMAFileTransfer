package com.basic.rdma.input;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * locate com.basic.rdma.input
 * Created by master on 2019/8/25.
 */
public class DataInputFormatTest {
    public static final DataInputFormat inputFormat=new DataInputFormat(1024*1024L);

    @Test
    public void getSplits() throws IOException {
        for (InputSplit split : inputFormat.getSplits("/Users/master/Downloads/Termius.dmg")) {
            System.out.println(split);
        }
    }
}
