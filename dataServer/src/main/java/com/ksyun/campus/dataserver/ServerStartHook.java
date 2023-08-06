package com.ksyun.campus.dataserver;

import cn.hutool.json.JSONUtil;
import com.ksyun.campus.dataserver.util.DataServerInfoUtil;
import dto.DataServerInstance;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
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
    private String basePath;  // 文件的最顶层目录，如：/data/

    @Autowired
    private CuratorFramework client;

    @Resource
    private DataServerInfoUtil serverInfoUtil;

    // zk中存储dataSever信息的目录，父节点不存在则创建
    private String DataServerInfoParentPath = "/MinFS/dataServerInfo";

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("服务Hook运行...");
        // 检查目录
        if (client.checkExists().forPath(DataServerInfoParentPath) == null) {
            client.create().creatingParentsIfNeeded().forPath(DataServerInfoParentPath);
        }

        long restCapacity = serverInfoUtil.getRestCapacity();

        DataServerInstance instance = DataServerInstance.builder()
                .ip(serverInfoUtil.getIp())
                .port(serverInfoUtil.getPort())
                .capacity(restCapacity) // 默认每个dataServer只能存储100MB
                .rack("rack1")
                .zone("zone1")
                .build();

        // 创建自动编号的临时顺序节点
        String path = client.create().withMode(CreateMode.EPHEMERAL)
                .forPath(DataServerInfoParentPath +"/"+ instance.getIp() + ":" + instance.getPort(),
                        JSONUtil.parse(instance).toString().getBytes(StandardCharsets.UTF_8));

        log.info("创建节点：{}", path);
        log.info("内容：{}", instance.toString());
    }
}
