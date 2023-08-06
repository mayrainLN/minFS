package com.ksyun.campus.dataserver.services;

import dto.RestResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Service
@Slf4j
public class DataService {
    @Value("${file.basePath}")
    private String basePathString;  // 如：/data/

    /**
     * 为减少复杂度只支持覆盖写，不支持追加写、随机写
     *
     * @param data
     * @param dirPathString  目录路径，如：ks3/，如果是base目录下，传入空字符串或者null
     * @param fileName 文件名，如：test.txt
     */
    public RestResult write(byte[] data, String dirPathString, String fileName) {

        boolean localWriteRes = writeLocalFile(data, dirPathString, fileName);
        //todo 写本地
        //todo 调用远程ds服务写接口，同步副本，已达到多副本数量要求
        //todo 选择策略，按照 az rack->zone 的方式选取，将三副本均分到不同的az下
        //todo 支持重试机制
        //todo 返回三副本位置
        return RestResult.fail();
    }


    public RestResult mkdir(String dirPathString) {

        boolean res = mkLocalDir(dirPathString);

        //todo 写本地
        //todo 调用远程ds服务写接口，同步副本，已达到多副本数量要求
        //todo 选择策略，按照 az rack->zone 的方式选取，将三副本均分到不同的az下
        //todo 支持重试机制
        //todo 返回三副本位置
        return RestResult.fail();
    }

    private boolean mkLocalDir(String dirPathString) {
        // base目录已经自带了'/'
        if(dirPathString.startsWith("/") || dirPathString.startsWith("\\")){
            dirPathString = dirPathString.substring(1);
        }
        File folder = new File(basePathString+dirPathString);
        if (folder.mkdirs()) {
            log.info("文件夹：{}创建成功", dirPathString);
            return true;
        }
        System.out.println("文件夹：{}创建失败" + dirPathString);
        return false;
    }

    public byte[] read(String path, int offset, int length) {

        Path filePath = Paths.get(basePathString, path);
        try {
            byte[] fileData = Files.readAllBytes(filePath);

            int readLength = Math.min(length, fileData.length - offset);
            if (readLength < 0) {
                readLength = 0;
            }
            byte[] resultData = new byte[readLength];

            System.arraycopy(fileData, offset, resultData, 0, readLength);

            return resultData;
        } catch (IOException e) {
            log.error("本地读取失败", e);
            e.printStackTrace();
            return null;
        }
    }

    private boolean writeLocalFile(byte[] data, String dirPathString, String fileName) {
        Path filePath;
        Path dirPath;
        if (dirPathString == null || dirPathString.isEmpty()) {
            // 没有指定目录，写在base目录下
            filePath = Paths.get(basePathString, fileName);
            dirPath = Paths.get(basePathString);
        } else {
            filePath = Paths.get(basePathString, dirPathString, fileName);
            dirPath = Paths.get(basePathString, dirPathString);
        }

        try {
            // 文件夹不存在先创建文件夹
            if(!Files.exists(dirPath)){
                Files.createDirectories(dirPath);
            }
            // 覆盖写：如果文件已经存在，则先删除原文件
            if (Files.exists(filePath)) {
                Files.delete(filePath);
            }
            Files.write(filePath, data);
            return true;
        } catch (IOException e) {
            log.error("本地写入失败", e);
            return false;
        }
    }
}
