package com.ksyun.campus.metaserver.controller;

import cn.hutool.json.JSONUtil;
import com.ksyun.campus.metaserver.client.DataServerClient;

import com.ksyun.campus.metaserver.domain.ReplicaData;
import com.ksyun.campus.metaserver.domain.StatInfo;
import com.ksyun.campus.metaserver.services.MetaService;
import dto.DataServerInstance;
import dto.PrefixConstants;
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
     * 1. 负载均衡创建文件 | 按照元信息中的路径重新创建文件
     * 2. 创建/修改文件的元数据信息
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


        if(fileSystem == null){
            fileSystem = "";
        }
        String fileLogicPath = fileSystem + path;
        String targetMataDataPath = PrefixConstants.ZK_PATH_FILE_META_INFO + fileLogicPath;
        List<DataServerInstance> dataServerInstanceList = null;

        /**
         * 判断文件是否存在
         */
        if(client.checkExists().forPath(targetMataDataPath)!=null){
            // 源文件存在,按照原路径修改源文件
            byte[] bytes = client.getData().forPath(targetMataDataPath);
            StatInfo statInfo = JSONUtil.toBean(new String(bytes), StatInfo.class);
            List<ReplicaData> replicaDatas = statInfo.getReplicaData();
            // 文件所在dataServer
            dataServerInstanceList = this.getInstancesListFromRelicaDatas(replicaDatas);
            log.info("文件已存在,即将修改文件");
        }else{
            // 源文件不存在,选出三个DataServer创建新文件
            dataServerInstanceList = metaService.pickDataServerToWrite();
            log.info("文件不存在,即将创建文件");
        }

        /**
         * 写入/修改文件
         */
        // 存储 dataServer 和 真实路径 的映射关系,写入/修改文件 的时候会同步更新pathMap
        Map<DataServerInstance, String> pathMap = new HashMap<>();
        ResponseEntity responseEntity = metaService.writeFileToDataServer(dataServerInstanceList, fileSystem, path, file,pathMap);
        if(!responseEntity.getStatusCode().is2xxSuccessful()){
            return (ResponseEntity) ResponseEntity.internalServerError();
        }

        /**
         * 写入/修改文件元数据
         */
        ResponseEntity updateMetaDataRes = metaService.updateMetaData(fileSystem, path, file, dataServerInstanceList, pathMap);
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

    @SneakyThrows
    @RequestMapping("delete")
    public ResponseEntity delete(@RequestHeader(required = false) String fileSystem, @RequestParam String path){
        if(fileSystem == null){
            fileSystem = "";
        }
        path+=fileSystem;
        StatInfo fileMetaInfo = metaService.getFileMetaInfo(path);
        // 删除数据
        for (ReplicaData replicaData : fileMetaInfo.getReplicaData()) {
            String dsNode = replicaData.getDsNode();// localhost:8000
            String host = dsNode.split(":")[0];
            int port = Integer.parseInt(dsNode.split(":")[1]);
            DataServerInstance dataServerInstance = DataServerInstance.builder()
                    .host(host)
                    .port(port)
                    .build();
            dataServerClient.deleteFile(dataServerInstance, fileSystem, path);
        }

        path = PrefixConstants.ZK_PATH_FILE_META_INFO + path;
        // TODO 删除元数据
        client.delete().forPath(path);
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

    private List<DataServerInstance>  getInstancesListFromRelicaDatas(List<ReplicaData> replicaDatas){
        List<DataServerInstance> dataServerInstances = new ArrayList<>();
        for (ReplicaData replicaData : replicaDatas) {
            String dsNode = replicaData.getDsNode();// localhost:8000
            String host = dsNode.split(":")[0];
            int port = Integer.parseInt(dsNode.split(":")[1]);
            DataServerInstance dataServerInstance = DataServerInstance.builder()
                    .host(host)
                    .port(port)
                    .build();
            dataServerInstances.add(dataServerInstance);
        }
        return dataServerInstances;
    }
}
