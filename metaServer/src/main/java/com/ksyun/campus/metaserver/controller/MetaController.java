package com.ksyun.campus.metaserver.controller;

import cn.hutool.Hutool;
import cn.hutool.json.JSON;
import cn.hutool.json.JSONUtil;
import com.ksyun.campus.metaserver.client.DataServerClient;

import com.ksyun.campus.metaserver.domain.ReplicaData;
import com.ksyun.campus.metaserver.domain.StatInfo;
import com.ksyun.campus.metaserver.services.MetaService;
import dto.DataServerInstance;
import dto.PrefixConstants;
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


/**
 * 所有的FileSystem参数已经在Client做好了处理。
 * 在Controller层直接拼接FileSystem和Path即可。
 */
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
    public ResponseEntity stats(@RequestHeader String fileSystem, @RequestParam String path) {
        return new ResponseEntity(HttpStatus.OK);
    }

    /**
     * 1. 负载均衡创建文件 | 按照元信息中的路径重新创建文件
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
            @RequestParam("path") String path) {
        String logicPath = getLogicPath(fileSystem, path);
        return metaService.createFile(logicPath);
    }


    /**
     * 创建目录
     *
     * @param fileSystem
     * @param path
     * @return
     */
    @RequestMapping("mkdir")
    public ResponseEntity mkdir(@RequestHeader(required = false) String fileSystem, @RequestParam String path) {
        path = getLogicPath(fileSystem, path);
        return metaService.mkdir(path);
    }

    @RequestMapping("listdir")
    public ResponseEntity listdir(@RequestHeader String fileSystem, @RequestParam String path) {
        return new ResponseEntity(HttpStatus.OK);
    }

    @SneakyThrows
    @RequestMapping("delete")
    public ResponseEntity delete(@RequestHeader(required = false) String fileSystem, @RequestParam String path) {
        path = getLogicPath(fileSystem, path);
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
            dataServerClient.deleteFile(dataServerInstance, path);
        }

        path = PrefixConstants.ZK_PATH_FILE_META_INFO + path;
        client.delete().forPath(path);
        return ResponseEntity.ok().body("删除成功");
    }

    /**
     * 追加写。写之前要检查文件是否已经被Commit
     * 依据就是ZK中的replica
     * 中间过程不需要改动元数据信息，只有在create和commit的时候才需要改动元数据信息
     *
     * @param fileSystem
     * @param path
     * @return
     */
    @SneakyThrows
    @RequestMapping("write")
    public ResponseEntity write(@RequestHeader(required = false) String fileSystem, @RequestParam String path, @RequestParam("file") MultipartFile file) {
        String fileLogicPath = getLogicPath(fileSystem, path);
        String targetMataDataPath = PrefixConstants.ZK_PATH_FILE_META_INFO + fileLogicPath;
        List<DataServerInstance> dataServerInstanceList = null;
        StatInfo statInfo = null;
        /**
         * 判断文件是否commit
         */
        if (client.checkExists().forPath(targetMataDataPath) != null) {
            byte[] bytes = client.getData().forPath(targetMataDataPath);
            statInfo = JSONUtil.toBean(new String(bytes), StatInfo.class);
            if (statInfo.isCommitted()) {
                return ResponseEntity.badRequest().body("文件已经体检,不允许修改,请删除后重新Create");
            }
            dataServerInstanceList = getInstancesListFromRelicaDatas(statInfo.getReplicaData());
            return metaService.appendReplica(dataServerInstanceList, path, file);
        } else {
            return ResponseEntity.badRequest().body("文件不存在,请先Create");
        }

    }

    /**
     * 修改文件的元数据信息
     *
     * @param fileSystem
     * @param path
     * @return
     */
    @SneakyThrows
    @RequestMapping("commit")
    public ResponseEntity commit(@RequestHeader(required = false) String fileSystem, @RequestParam String path) {
        String fileLogicPath = getLogicPath(fileSystem, path);
        String targetMataDataPath = PrefixConstants.ZK_PATH_FILE_META_INFO + fileLogicPath;
        if (client.checkExists().forPath(targetMataDataPath) == null) {
            return ResponseEntity.badRequest().body("文件未创建,请先Create");
        }
        byte[] bytes = client.getData().forPath(targetMataDataPath);
        String json = new String(bytes);
        StatInfo statInfo = JSONUtil.toBean(json, StatInfo.class);
        if (statInfo.isCommitted()) {
            return ResponseEntity.badRequest().body("文件已经提交,请勿重复提交");
        }
        statInfo.setCommitted(true);
        statInfo.setMtime(System.currentTimeMillis());
        statInfo.setSize(999999);
        client.setData().forPath(targetMataDataPath, JSONUtil.toJsonStr(statInfo).getBytes());
        return ResponseEntity.ok().body("文件提交成功");
    }

    /**
     * 根据文件path查询三副本的位置，返回客户端具体ds、文件分块信息
     *
     * @param fileSystem
     * @param path
     * @return
     */
    @RequestMapping("open")
    public ResponseEntity open(@RequestHeader String fileSystem, @RequestParam String path) {
        return null;
    }

    /**
     * 关闭退出进程
     */
    @RequestMapping("shutdown")
    public void shutdownServer() {
        System.exit(-1);
    }

    private List<DataServerInstance> getInstancesListFromRelicaDatas(List<ReplicaData> replicaDatas) {
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

    /**
     * 保证最后的逻辑地址是/a/b 或者a/b/x.txt
     * @param fileSystem
     * @param path
     * @return
     */
    private String getLogicPath(String fileSystem, String path) {
        if (fileSystem == null) {
            fileSystem = "";
        }else{
            if(!fileSystem.startsWith("/")){
                fileSystem = "/" + fileSystem;
            }
            if(fileSystem.endsWith("/")){
                fileSystem = fileSystem.substring(0,fileSystem.length()-1);
            }
        }
        if(!path.startsWith("/")){
            path = "/" + path;
        }
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        return fileSystem + path;
    }

}
