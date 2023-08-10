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
import java.util.stream.Collectors;

/**
 * Controller层已经处理过了FileSystem，path已经包含了FileSystem
 */
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
     * @param dataServerInstances 副本所在的三个dataServer
     * @param path
     * @param file                新文件
     * @return
     */
    public ResponseEntity appendReplica(List<DataServerInstance> dataServerInstances,
                                        String path,
                                        MultipartFile file) {
        // 请求dataServer创建文件
        // TODO 创建文件这里可以并发请求，用Future接收。 可以用CountDownLatch
        for (DataServerInstance dataServerInstance : dataServerInstances) {
            //TODO 支持重试机制
            ResponseEntity responseEntity = dataServerClient.appendReplicas(dataServerInstance, path, file);
            if (responseEntity.getStatusCode() != HttpStatus.OK) {
                log.error("追加写入失败");
                return ResponseEntity.internalServerError().body("追加写入失败");
            }
        }
        return ResponseEntity.ok().body("追加写入成功");
    }

    @SneakyThrows
    public ResponseEntity updateMetaData(String path, List<DataServerInstance> dataServerInstances, Map<DataServerInstance, String> pathMap) {

        List<ReplicaData> replicaDataList = new ArrayList<>();
        for (DataServerInstance dataServerInstance : dataServerInstances) {
            ReplicaData replicaData = new ReplicaData(dataServerInstance)
                    .setPath(pathMap.get(dataServerInstance)) //存物理路径
                    .setId(UUID.randomUUID().toString());
            replicaDataList.add(replicaData);
        }
        // 创建文件的元数据信息
        StatInfo fileStateInfo = StatInfo.builder()
                .path(path) // 存逻辑路径
                .type(FileType.File)
                .replicaData(replicaDataList)
                .build();

        String targetMataDataPath = PrefixConstants.ZK_PATH_FILE_META_INFO + path;
        if (client.checkExists().forPath(targetMataDataPath) == null) {
            // 没有元信息，说明是创建文件。创建元信息
//            log.info("创建元数据：{}", fileStateInfo.toString());
            createNodeRecursively(targetMataDataPath, fileStateInfo);
        } else {
            // 修改元信息
//            log.info("修改元数据：{}", fileStateInfo.toString());
            client.setData().forPath(targetMataDataPath, JSONUtil.parse(fileStateInfo).toString().getBytes(StandardCharsets.UTF_8));
        }

        return new ResponseEntity(HttpStatus.OK);
    }

    /**
     * 递归地创建元信息
     */
    @SneakyThrows
    private void createNodeRecursively(String path, StatInfo statInfo) {
        String[] parts = path.split("/");
        StringBuilder partialPath = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            if (!part.isEmpty()) {
                partialPath.append("/").append(part);
                // 创建数据节点/或者最后一层的目录
                if (i == parts.length - 1) {
                    client.create()
                            .withMode(CreateMode.PERSISTENT)
                            .forPath(partialPath.toString(),
                                    JSONUtil.parse(statInfo).toString().getBytes(StandardCharsets.UTF_8));
                } else if (client.checkExists().forPath(partialPath.toString()) == null) {
                    int prefixLen = PrefixConstants.ZK_PATH_FILE_META_INFO.length();
                    // 上层目录,且不存在,创建目录需要手动创建
                    StatInfo dirStatInfo = StatInfo.builder()
                            .path(partialPath.substring(prefixLen)) // 存逻辑路径
                            .type(FileType.Directory)
                            .mtime(System.currentTimeMillis())
                            .isCommitted(true)
                            .size(0L)
                            .build();
                    // 创建不存在的目录
                    client.create().
                            creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                            .forPath(partialPath.toString(),
                                    JSONUtil.parse(dirStatInfo).toString().getBytes(StandardCharsets.UTF_8)
                            );
                }
            }
        }
    }

    /**
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

    @SneakyThrows
    public ResponseEntity createFile(String logicPath) {
        List<DataServerInstance> dataServerInstanceList = this.pickDataServerToWrite();
        // 请求dataServer创建文件
        Map<DataServerInstance, String> realPathMap = new HashMap<>();
        // TODO 创建文件这里可以并发请求，用Future接收。 可以用CountDownLatch
        for (DataServerInstance dataServerInstance : dataServerInstanceList) {
            //TODO 支持重试机制
            ResponseEntity responseEntity = dataServerClient.createFileOnDataServer(dataServerInstance, logicPath);
            if (responseEntity.getStatusCode() != HttpStatus.OK) {
                return responseEntity;
            }
            // 存储文件位于dataServer上的真实路径
            realPathMap.put(dataServerInstance, responseEntity.getBody().toString());
        }
        return updateMetaData(logicPath, dataServerInstanceList, realPathMap);
    }

    /**
     * 不会去DataServer中创建文件夹。只会在zk中创建元数据。
     * 1. 文件夹没有内容，没有意义。
     * 2. 文件夹里的文件是负载均衡的，均匀分布在四个DataServer中。
     * 3. 所以创建真实文件夹这个步骤，可以延迟到创建文件的时候再去做。
     *
     * @param path
     * @return
     */
    @SneakyThrows
    public ResponseEntity mkdir(String path) {
        String targetMataDataPath = PrefixConstants.ZK_PATH_FILE_META_INFO + path;
        if (client.checkExists().forPath(targetMataDataPath) == null) {
            // 没有元信息，说明是创建文件。创建元信息
            StatInfo fileStateInfo = StatInfo.builder()
                    .path(path) // 存逻辑路径
                    .type(FileType.Directory)
                    .mtime(System.currentTimeMillis())
                    .isCommitted(true)
                    .size(0L)
                    .build();
            createNodeRecursively(targetMataDataPath, fileStateInfo);
        } else {
            return ResponseEntity.badRequest().body("文件夹已存在");
        }
        return ResponseEntity.ok().body("创建文件夹成功");
    }
}
