package com.ksyun.campus.dataserver.util;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.net.InetAddress;

/**
 * @author :MayRain
 * @version :1.0
 * @date :2023/8/5 16:51
 * @description :
 */
@Component
@Slf4j
public class DataServerInfoUtil {
    /**
     * 默认每个dataServer只能存储100MB
     */
    public static final long  MAX_DATA_CAPACITY = 104857600L;


    @Value("${file.basePath}")
    private String basePath;  // 文件的最顶层目录，如：/data/

    @Value("${server.port}")
    private int serverPort;

    @SneakyThrows
    public String getIp() {
        return InetAddress.getLocalHost().getHostAddress();
        // 防止ipv4地址出意外，直接写本机吧
//        return "localhost";
    }

    public int getPort() {
        return serverPort;
    }

    /**
     * 获取本DataServer实例真实的文件存储base路径
     */
    public String getRealBasePath() {
        return basePath+"/"+getIp()+"-"+getPort();
    }

    /**
     * 获取本DataServer实例剩余容量
     */
     public long getRestCapacity() {
        File folder = new File(getRealBasePath());
        if(!folder.exists()) {
            log.info("base目录不存在，创建目录: {}", folder.getAbsolutePath());
            folder.mkdirs();
        }
        long folderSizeBytes  = FileUtils.sizeOfDirectory(folder);
        long restCapacity = MAX_DATA_CAPACITY - folderSizeBytes;
        log.info("当前DataServer剩余容量: {}", restCapacity);
        return restCapacity;
    }

}

