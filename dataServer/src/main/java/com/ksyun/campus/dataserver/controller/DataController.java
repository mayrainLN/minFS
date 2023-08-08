package com.ksyun.campus.dataserver.controller;

import cn.hutool.json.JSONUtil;
import com.ksyun.campus.dataserver.services.DataService;
import com.ksyun.campus.dataserver.util.DataServerInfoUtil;
import dto.DataServerInstance;
import dto.PrefixConstants;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import java.io.IOException;

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
     * 1、保存在本地磁盘下的文件内
     * 2、返回
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
        long size = file.getSize();
        String dataServerInfoPath = PrefixConstants.ZK_PATH_DATA_SERVER_INFO + "/" + dataServerInfoUtil.getIp() + ":" + dataServerInfoUtil.getPort();
        String jsonStr = new String(client.getData().forPath(dataServerInfoPath));
        DataServerInstance dataServerInfo = JSONUtil.toBean(jsonStr, DataServerInstance.class);

        /**
         * 修改元数据中本机剩余容量
         *
         */
        //TODO 这里仅仅把所有写当做是第一次写入，单调地减少剩余容量
        //TODO 实际上，如果本地已经有这个文件了， 就要检查文件的大小，再计算本地容量的增减情况

        //TODO 不只是写入文件要这样考虑，还有删除文件，甚至删除文件夹也是

        //TODO 真要做起来，还不如直接用100M - 根目录文件夹大小。缺点就是只适用于本题目。
        dataServerInfo.setCapacity(dataServerInfo.getCapacity() - size);
        dataService.updateMetaData(dataServerInfoPath, dataServerInfo);

        ResponseEntity responseEntity = dataService.writeLocalFile(fileSystem, path, bytes);
        if(!responseEntity.getStatusCode().is2xxSuccessful()){
            return (ResponseEntity) ResponseEntity.internalServerError();
        }
        return responseEntity;
    }

    @RequestMapping("delete")
    public ResponseEntity deleteFile(@RequestHeader(required = false) String fileSystem,
                                    @RequestParam("path") String path) {
        return dataService.deleteLocalFile(fileSystem, path);
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
