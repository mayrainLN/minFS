package com.ksyun.campus.metaserver.controller;

import com.ksyun.campus.metaserver.client.DataServerClient;

import com.ksyun.campus.metaserver.services.MetaService;
import dto.DataServerInstance;
import dto.RestResult;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;

@RestController("/")
@Slf4j
public class MetaController {
    @Resource
    CuratorFramework client;

    @Resource
    MetaService metaService;

    @Resource
    DataServerClient dataServerClient;

    @RequestMapping("stats")
    public ResponseEntity stats(@RequestHeader String fileSystem,@RequestParam String path){
        return new ResponseEntity(HttpStatus.OK);
    }

    /**
     * 1. 创建文件
     * 2. 创建/修改文件的元数据信息
     *
     * @param fileSystem
     * @param path
     * @return
     */
    @SneakyThrows
    @RequestMapping("create")
    public ResponseEntity createFile(
            @RequestHeader(required = false) String fileSystem,
            @RequestParam("path") String path,
            @RequestParam("file") MultipartFile file){

        // 选择三个DataServer
        List<DataServerInstance> dataServerInstances = metaService.pickDataServerToWrite();

        byte[] bytes = null;
        try {
            bytes = file.getBytes();
        } catch (IOException e) {
            log.error("文件内容读取错误");
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.BAD_REQUEST);
        }

        Map<DataServerInstance, String> pathMap = new HashMap<>();
        // 请求dataServer创建文件
        // TODO 创建文件这里可以并发请求，用Future接收。 可以用CountDownLatch
        for (DataServerInstance dataServerInstance : dataServerInstances) {
            //TODO 支持重试机制
            ResponseEntity responseEntity = dataServerClient.writeFileToDataServer(dataServerInstance, fileSystem, path, file);
            if(responseEntity.getStatusCode()!=HttpStatus.OK){
                return (ResponseEntity) ResponseEntity.internalServerError();
            }
            pathMap.put(dataServerInstance, responseEntity.getBody().toString());
        }

        // 更新元数据
        ResponseEntity updateMetaDataRes = metaService.updateMetaData(fileSystem, path, file, dataServerInstances, pathMap);
        return updateMetaDataRes;
    }

    /**
     * 创建目录
     * @param fileSystem
     * @param path
     * @return
     */
    @RequestMapping("mkdir")
    public ResponseEntity mkdir(@RequestHeader String fileSystem, @RequestParam String path){
        return new ResponseEntity(HttpStatus.OK);
    }

    @RequestMapping("listdir")
    public ResponseEntity listdir(@RequestHeader String fileSystem,@RequestParam String path){
        return new ResponseEntity(HttpStatus.OK);
    }

    @RequestMapping("delete")
    public ResponseEntity delete(@RequestHeader String fileSystem, @RequestParam String path){
        return new ResponseEntity(HttpStatus.OK);
    }

    /**
     * 提交写
     * client写完文件后，向MetaServer提交。
     * 保存文件写入成功后的元数据信息，包括文件path、size、三副本信息等
     * @param fileSystem
     * @param path
     * @return
     */
    @RequestMapping("write")
    public RestResult commitWrite(@RequestHeader String fileSystem, @RequestParam String path){

        return null;
    }

    /**
     * 根据文件path查询三副本的位置，返回客户端具体ds、文件分块信息
     * @param fileSystem
     * @param path
     * @return
     */
    @RequestMapping("open")
    public RestResult open(@RequestHeader String fileSystem, @RequestParam String path){
        return new RestResult();
    }

    /**
     * 关闭退出进程
     */
    @RequestMapping("shutdown")
    public void shutdownServer(){
        System.exit(-1);
    }
}
