package com.basic.rdma.input;

import java.io.IOException;
import java.nio.file.Path;

/**
 * locate com.basic.rdma.input
 * Created by master on 2019/8/25.
 */
public abstract class InputSplit {
    /**
     * Get the size of the split, so that the input splits can be sorted by size.
     * @return the number of bytes in the split
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract long getLength() throws IOException, InterruptedException;

    public abstract long getStart() throws IOException, InterruptedException;;

    public abstract Path getPath() throws IOException, InterruptedException;;
}
