package com.ksyun.campus.dataserver.controller;

import com.ksyun.campus.dataserver.services.DataService;
import com.ksyun.campus.dataserver.util.DataServerInfoUtil;
import dto.RestResult;
import dto.WriteFileRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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
     * @return
     */
    @RequestMapping("write")
    public RestResult writeFile(@RequestHeader(required = false) String fileSystem,
                                @RequestBody WriteFileRequest request){
        if (request == null || request.getPath() == null) {
            return RestResult.fail();
        }
        return dataService.writeFile(fileSystem,request.getPath(),request.getData().getBytes());
    }

    @GetMapping("read")
    public RestResult readFile(@RequestHeader(required = false)  String fileSystem, @RequestParam String path, @RequestParam int offset, @RequestParam int length){
        return dataService.readFile(fileSystem,path,offset,length);
    }
 
    /**
     * 关闭退出进程
     */
    @RequestMapping("shutdown")
    public void shutdownServer(){
        System.exit(-1);
    }
}
