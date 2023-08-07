package com.ksyun.campus.dataserver.controller;

import com.ksyun.campus.dataserver.services.DataService;
import com.ksyun.campus.dataserver.util.DataServerInfoUtil;
import dto.RestResult;
import dto.WriteFileRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import java.io.IOException;
import java.io.InputStream;

@RestController("/")
@Slf4j
public class DataController {
    @Resource
    DataService dataService;

    /**
     * 1、保存在本地磁盘下的文件内
     * 2、返回
     *
     * @param fileSystem fileSystem，相当于namespace，来模拟多个volume，相当与C盘D盘
     * @return
     */
    @RequestMapping("write")
    public ResponseEntity writeFile(@RequestHeader(required = false) String fileSystem,
                                    @RequestParam("path") String path,
                                    @RequestParam("file") MultipartFile file) {
        if (file == null) {
            return new ResponseEntity(HttpStatus.BAD_REQUEST);
        }
        byte[] bytes = null;
        try {
            bytes = file.getBytes();
        } catch (IOException e) {
            e.printStackTrace();
        }
        long size = file.getSize();
        // TODO 同步更新当前dataServer容量
        boolean res = dataService.updateCapacity(size);
        return dataService.writeFile(fileSystem, path, bytes);
    }

    @GetMapping("read")
    public ResponseEntity readFile(@RequestHeader(required = false) String fileSystem, @RequestParam String path, @RequestParam int offset, @RequestParam int length) {
        return dataService.readFile(fileSystem, path, offset, length);
    }

    /**
     * 关闭退出进程
     */
    @RequestMapping("shutdown")
    public void shutdownServer() {
        System.exit(-1);
    }
}
