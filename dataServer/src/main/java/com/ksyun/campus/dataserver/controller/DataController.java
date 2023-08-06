package com.ksyun.campus.dataserver.controller;

import com.ksyun.campus.dataserver.services.DataService;
import dto.RestResult;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@RestController("/")
public class DataController {
    @Resource
    DataService dataService;
    /**
     * 1、读取request content内容并保存在本地磁盘下的文件内
     * 2、同步调用其他ds服务的write，完成另外2副本的写入
     * 3、返回写成功的结果及三副本的位置
     * @param fileSystem fileSystem，相当于namespace，来模拟多个volume，相当与C盘D盘
     * @param path
     * @return
     */
    @RequestMapping("write")
    public RestResult writeFile(@RequestHeader String fileSystem, @RequestParam String path){
        return null;
    }

    /**
     * 在指定本地磁盘路径下，读取指定大小的内容后返回
     * @param fileSystem
     * @param path
     * @return
     */
    @RequestMapping("read")
    public RestResult readFile(@RequestHeader String fileSystem, @RequestParam String path){
        return null;
    }
 
    /**
     * 关闭退出进程
     */
    @RequestMapping("shutdown")
    public void shutdownServer(){
        System.exit(-1);
    }
}
