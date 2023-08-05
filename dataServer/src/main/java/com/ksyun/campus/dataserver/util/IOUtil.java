package com.ksyun.campus.dataserver.util;

import org.apache.commons.io.FileUtils;

import java.io.File;

/**
 * @author :MayRain
 * @version :1.0
 * @date :2023/8/5 17:17
 * @description :
 */
public class IOUtil {
    public static long getFolderSize(String path){
        File folder = new File(path);
        return FileUtils.sizeOfDirectory(folder);
    }
}
