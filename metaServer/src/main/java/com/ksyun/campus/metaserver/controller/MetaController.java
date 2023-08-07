package com.ksyun.campus.metaserver.controller;

import com.ksyun.campus.metaserver.client.DataServerClient;
import com.ksyun.campus.metaserver.util.LoadBalanceUtil;
import dto.DataServerInstance;
import dto.RestResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.*;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Objects;

@RestController("/")
@Slf4j
public class MetaController {
    @Resource
    LoadBalanceUtil loadBalanceUtil;

    @Resource
    DataServerClient dataServerClient;

    @RequestMapping("stats")
    public ResponseEntity stats(@RequestHeader String fileSystem,@RequestParam String path){
        return new ResponseEntity(HttpStatus.OK);
    }

    /**
     * 创建文件，主要是创建文件的元数据信息
     * @param fileSystem
     * @param path
     * @return
     */
    @RequestMapping("create")
    public ResponseEntity createFile(
            @RequestHeader(required = false) String fileSystem,
            @RequestParam("path") String path,
            @RequestParam("file") MultipartFile file){
        List<DataServerInstance> dataServerInstances = loadBalanceUtil.loadBalance();

        byte[] bytes = null;
        try {
            bytes = file.getBytes();
        } catch (IOException e) {
            log.error("文件内容读取错误");
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.BAD_REQUEST);
        }
        // 请求dataServer创建文件
        // TODO 创建文件这里可以并发请求，用Future接收。 可以用CountDownLatch
        for (DataServerInstance dataServerInstance : dataServerInstances) {
            ResponseEntity responseEntity = dataServerClient.writeFileToDataServer(dataServerInstance, fileSystem, path, file);
            if(responseEntity.getStatusCode()!=HttpStatus.OK){
                return (ResponseEntity) ResponseEntity.internalServerError();
            }
        }
        return new ResponseEntity(HttpStatus.OK);
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
