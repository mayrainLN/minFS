package com.ksyun.campus.dataserver;

import cn.hutool.json.JSONUtil;
import com.ksyun.campus.dataserver.util.ServerInfoUtil;
import dto.DataServerInstance;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.File;
import java.nio.charset.StandardCharsets;

/**
 * @author :MayRain
 * @version :1.0
 * @date :2023/8/5 16:25
 * @description :
 */
@Slf4j
@Component
public class ServerStartHook implements ApplicationRunner {
    @Value("${file.basePath}")
    private String basePath;  // 如：/data/

    @Autowired
    private CuratorFramework client;

    @Resource
    private ServerInfoUtil serverInfoUtil;

    // 父节点不存在则创建
    private String parentPath = "/dataServer";

    public static final long  MAX_DATA_CAPACITY = 104857600L; // 默认每个dataServer只能存储100MB
    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("服务Hook运行...");
        // 检查目录
        if (client.checkExists().forPath(parentPath) == null) {
            client.create().creatingParentsIfNeeded().forPath(parentPath);
        }

        File folder = new File(basePath);
        long folderSizeBytes  = FileUtils.sizeOfDirectory(folder);
        long restCapacity = MAX_DATA_CAPACITY - folderSizeBytes;
        log.info("当前DataServer剩余容量: {}", restCapacity);

        DataServerInstance instance = DataServerInstance.builder()
                .ip(serverInfoUtil.getIp())
                .port(serverInfoUtil.getPort())
                .capacity(restCapacity) // 默认每个dataServer只能存储100MB
                .rack("rack1")
                .zone("zone1")
                .build();

        // 创建自动编号的临时顺序节点
        String path = client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath(parentPath + "/", JSONUtil.parse(instance).toString().getBytes(StandardCharsets.UTF_8));

        log.info("创建节点：{}", path);
        log.info("内容：{}", instance.toString());
    }
}
