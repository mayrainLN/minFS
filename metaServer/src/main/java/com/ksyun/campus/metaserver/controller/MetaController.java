package com.ksyun.campus.metaserver.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController("/")
public class MetaController {
    @RequestMapping("stats")
    public ResponseEntity stats(@RequestHeader String fileSystem,@RequestParam String path){
        return new ResponseEntity(HttpStatus.OK);
    }
    @RequestMapping("create")
    public ResponseEntity createFile(@RequestHeader String fileSystem, @RequestParam String path){
        return new ResponseEntity(HttpStatus.OK);
    }
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
     * 保存文件写入成功后的元数据信息，包括文件path、size、三副本信息等
     * @param fileSystem
     * @param path
     * @param offset
     * @param length
     * @return
     */
    @RequestMapping("write")
    public ResponseEntity commitWrite(@RequestHeader String fileSystem, @RequestParam String path, @RequestParam int offset, @RequestParam int length){
        return new ResponseEntity(HttpStatus.OK);
    }

    /**
     * 根据文件path查询三副本的位置，返回客户端具体ds、文件分块信息
     * @param fileSystem
     * @param path
     * @return
     */
    @RequestMapping("open")
    public ResponseEntity open(@RequestHeader String fileSystem,@RequestParam String path){
        return new ResponseEntity(HttpStatus.OK);
    }

    /**
     * 关闭退出进程
     */
    @RequestMapping("shutdown")
    public void shutdownServer(){
        System.exit(-1);
    }

}
