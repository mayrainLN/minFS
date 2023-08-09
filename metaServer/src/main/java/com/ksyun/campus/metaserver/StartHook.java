package com.ksyun.campus.metaserver;

import cn.hutool.json.JSONUtil;
import com.ksyun.campus.metaserver.domain.MetaServerMsg;
import dto.PrefixConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @author :MayRain
 * @version :1.0
 * @date :2023/8/6 18:41
 * @description :
 */
@Slf4j
@Component
public class StartHook implements ApplicationRunner {
    @Resource
    CuratorFramework client;

    @Value("${server.port}")
    private int serverPort;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("MetaServer Hook运行...");
        // 检查目录
        String basePath = PrefixConstants.ZK_PATH_META_SERVER_INFO;
        if (client.checkExists().forPath(basePath) == null) {
            client.create().creatingParentsIfNeeded().forPath(basePath);
        }

        MetaServerMsg serverMsg = new MetaServerMsg();
        serverMsg.setHost("localhost");
        serverMsg.setPort(serverPort);
        String zNodePath = client.create().
                withMode(CreateMode.EPHEMERAL_SEQUENTIAL).
                forPath(basePath + "/node-");
        client.setData().forPath(zNodePath, JSONUtil.toJsonStr(serverMsg).getBytes());
    }
}
