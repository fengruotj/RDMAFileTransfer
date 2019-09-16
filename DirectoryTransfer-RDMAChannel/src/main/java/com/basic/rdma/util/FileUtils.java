package com.basic.rdma.util;

import java.io.File;

/**
 * locate com.basic.rdma.util
 * Created by MasterTj on 2019/9/16.
 */
public class FileUtils {
    /**
     * 删除整个文件夹
     * @param file
     */
    public static void delDir(File file) {
        if (file.isDirectory()) {
            File zFiles[] = file.listFiles();
            for (File file2 : zFiles) {
                delDir(file2);
            }
            file.delete();
        } else {
            file.delete();
        }
    }

}
