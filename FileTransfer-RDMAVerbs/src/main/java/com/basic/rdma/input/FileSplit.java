package com.basic.rdma.input;

import java.nio.file.Path;

/**
 * locate com.basic.rdma.input
 * Created by master on 2019/8/25.
 */
public class FileSplit extends InputSplit{
    private Path file;
    private long start;
    private long length;

    public FileSplit() {}

    /** Constructs a split with host information
     *
     * @param file the file name
     * @param start the position of the first byte in the file to process
     * @param length the number of bytes in the file to process
     */
    public FileSplit(Path file, long start, long length) {
        this.file = file;
        this.start = start;
        this.length = length;
    }

    /** The file containing this split's data. */
    public Path getPath() { return file; }

    /** The position of the first byte in the file to process. */
    public long getStart() { return start; }

    /** The number of bytes in the file to process. */
    public long getLength() { return length; }

    @Override
    public String toString() { return file + ":" + start + "+" + length; }
}
