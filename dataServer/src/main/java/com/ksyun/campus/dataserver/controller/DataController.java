package com.ksyun.campus.dataserver.controller;

import com.ksyun.campus.dataserver.services.DataService;
import com.ksyun.campus.dataserver.util.DataServerInfoUtil;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;

/**
 * client已经处理过了，没有FileSystem的概念了
 */
@RestController("/")
@Slf4j
public class DataController {
    @Resource
    DataService dataService;

    @Resource
    CuratorFramework client;

    @Resource
    DataServerInfoUtil dataServerInfoUtil;

    /**
     * 仅仅创建文件
     */
    @SneakyThrows
    @RequestMapping("create")
    public ResponseEntity create(@RequestParam("path") String path) {
        return dataService.createFile(path);
    }

    /**
     * 追加写入文件
     *
     * @param fileSystem fileSystem，相当于namespace，来模拟多个volume，相当与C盘D盘
     * @return
     */
    @SneakyThrows
    @RequestMapping("write")
    public ResponseEntity writeFile(@RequestHeader(required = false) String fileSystem,
                                    @RequestParam("path") String path,
                                    @RequestParam("file") MultipartFile file) {
        if (file == null) {
            return new ResponseEntity(HttpStatus.BAD_REQUEST);
        }
        byte[] bytes = file.getBytes();

        /**
         * 在本机写入文件
         */
        ResponseEntity responseEntity = dataService.appendLocalFile(fileSystem, path, bytes);
        if(!responseEntity.getStatusCode().is2xxSuccessful()){
            return ResponseEntity.internalServerError().body("写入文件失败");
        }
        return responseEntity;
    }

    @RequestMapping("delete")
    public ResponseEntity deleteFile(@RequestHeader(required = false) String fileSystem,
                                    @RequestParam("path") String path) {
        return dataService.deleteLocalFile(fileSystem, path);
    }

    @RequestMapping("read")
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
