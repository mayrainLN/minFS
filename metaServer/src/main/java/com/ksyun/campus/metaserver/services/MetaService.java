package com.ksyun.campus.metaserver.services;

import cn.hutool.json.JSONUtil;
import com.ksyun.campus.metaserver.client.DataServerClient;
import com.ksyun.campus.metaserver.domain.FileType;
import com.ksyun.campus.metaserver.domain.ReplicaData;
import com.ksyun.campus.metaserver.domain.StatInfo;
import dto.DataServerInstance;
import dto.PrefixConstants;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import javax.annotation.Resource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Service
@Slf4j
public class MetaService {
    @Resource
    private CuratorFramework client;

    @Resource
    DataServerClient dataServerClient;

    /**
     * 选出3个dataServer，返回DataServerInstance
     * 暂时只是根据剩余容量来选择
     *
     * @return
     */
    // TODO 选择策略，按照 az rack->zone 的方式选取，将三副本均分到不同的az下
    public List<DataServerInstance> pickDataServerToWrite() {
        String basePath = PrefixConstants.ZK_PATH_DATA_SERVER_INFO;
        List<String> dataServerList = null;
        try {
            dataServerList = client.getChildren().forPath(basePath);
        } catch (Exception e) {
            log.error("获取dataServer列表信息失败,路径为:{}", basePath);
            e.printStackTrace();
        }
        //暂时负载策略是选出三个剩余容量最大的
        PriorityQueue<DataServerInstance> priorityQueue = new PriorityQueue(3,
                (x, y) -> {
                    DataServerInstance xx = (DataServerInstance) x;
                    DataServerInstance yy = (DataServerInstance) x;
                    return xx.getCapacity() < yy.getCapacity() ? 1 : -1;
                }
        );
        for (String child : dataServerList) {
            String childPath = basePath + "/" + child;
            String jsonStr = null;
            try {
                Stat stat = new Stat();
                byte[] data = client.getData().storingStatIn(stat).forPath(childPath);
                jsonStr = new String(data);
            } catch (Exception e) {
                log.error("获取dataServer信息失败,路径为:{}", childPath);
                e.printStackTrace();
            }
            log.info("dataServerInfo:{}", jsonStr);
            DataServerInstance instance = JSONUtil.toBean(jsonStr, DataServerInstance.class);
            if (priorityQueue.size() < 3) {
                priorityQueue.add(instance);
            } else {
                DataServerInstance peek = priorityQueue.peek();
                if (peek.getCapacity() < instance.getCapacity()) {
                    priorityQueue.poll();
                    priorityQueue.add(instance);
                }
            }
        }
        return new ArrayList<>(priorityQueue);
    }

    /**
     *
     * @param dataServerInstances 副本所在的三个dataServer
     * @param fileSystem
     * @param path
     * @param file 新文件
     * @return
     */
    public ResponseEntity writeFileToDataServer(List<DataServerInstance> dataServerInstances,
                                                String fileSystem,
                                                String path,
                                                MultipartFile file,
                                                Map<DataServerInstance, String> pathMap) {
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
            //TODO 支持重试机制
            ResponseEntity responseEntity = dataServerClient.writeFileToDataServer(dataServerInstance, fileSystem, path, file);
            if(responseEntity.getStatusCode()!=HttpStatus.OK){
                return (ResponseEntity) ResponseEntity.internalServerError();
            }
            pathMap.put(dataServerInstance, responseEntity.getBody().toString());
        }
        return new ResponseEntity(HttpStatus.OK);
    }

    @SneakyThrows
    public ResponseEntity updateMetaData(String fileSystem, String path, MultipartFile file, List<DataServerInstance> dataServerInstances, Map<DataServerInstance, String> pathMap) {
        if(fileSystem == null){
            fileSystem = "";
        }
        List<ReplicaData> replicaDataList = new ArrayList<>();
        for (DataServerInstance dataServerInstance : dataServerInstances) {
            ReplicaData replicaData = new ReplicaData(dataServerInstance)
                    .setPath(pathMap.get(dataServerInstance)) //存物理路径
                    .setId(UUID.randomUUID().toString());
            replicaDataList.add(replicaData);
        }
        // 创建文件的元数据信息
        StatInfo fileStateInfo = StatInfo.builder()
                .path(fileSystem + path) // 存逻辑路径
                .size(file.getSize())
                .mtime(System.currentTimeMillis())
                .type(FileType.File)
                .replicaData(replicaDataList)
                .build();

        if (fileSystem == null) {
            fileSystem = "";
        }
        String fileLogicPath = fileSystem + path;
        String targetMataDataPath = PrefixConstants.ZK_PATH_FILE_META_INFO + fileLogicPath;
        if(client.checkExists().forPath(targetMataDataPath)== null){
            // 没有元信息，说明是创建文件。创建元信息
//            log.info("创建元数据：{}", fileStateInfo.toString());
            String zNodePath = createNodeRecursively(targetMataDataPath, fileStateInfo);
        }else{
            // 修改元信息
//            log.info("修改元数据：{}", fileStateInfo.toString());
            client.setData().forPath(targetMataDataPath, JSONUtil.parse(fileStateInfo).toString().getBytes(StandardCharsets.UTF_8));
        }

        return new ResponseEntity(HttpStatus.OK);
    }

    @SneakyThrows
    private String createNodeRecursively(String path, StatInfo statInfo) {
        String[] parts = path.split("/");
        StringBuilder partialPath = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            if (!part.isEmpty()) {
                partialPath.append("/").append(part);
                // 创建数据节点
                if (i == parts.length - 1) {
                    String zNodePath = client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                            .forPath(partialPath.toString(),
                                    JSONUtil.parse(statInfo).toString().getBytes(StandardCharsets.UTF_8));
                    return zNodePath;
                } else if (client.checkExists().forPath(partialPath.toString()) == null) {
                    // 创建不存在的目录
                    client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                            .forPath(partialPath.toString()
                            );
                }
            }
        }
        return null;
    }

    /**
     *
     * @param path 以/开头
     * @return
     */
    public StatInfo getFileMetaInfo(String path) {
        String nodeFullPath = PrefixConstants.ZK_PATH_FILE_META_INFO + path;
        byte[] bytes = null;
        try {
            bytes = client.getData().forPath(nodeFullPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return JSONUtil.toBean(new String(bytes), StatInfo.class);
    }
}
