package com.ksyun.campus.client.util;

//import javax.annotation.PostConstruct;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.TimeUnit;


@Slf4j
public class ZkUtil {
    private static final int SESSION_TIMEOUT = 5000;
    //    private static final String PASSWORD = "ln20010110";
//    private static final String USERNAME = "root";
    private static final String ZOOKEEPER_HOST = "localhost:2181"; // 替换为您的Zookeeper服务器地址和端口号

    //    @PostConstruct  客户端不一定使用spring，所以不能使用@PostConstruct
    public void postCons() throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper(ZOOKEEPER_HOST, SESSION_TIMEOUT, event -> {
            // 监听器事件处理逻辑
            log.info("触发监听器事件：" + event.toString());

            // 节点事件类型处理
            if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                log.info("节点数据变化：" + event.getPath());
                // 处理节点数据变化的逻辑
                // ...
            } else if (event.getType() == Watcher.Event.EventType.NodeCreated) {
                log.info("节点被创建：" + event.getPath());
                // 处理节点创建的逻辑
                // ...
            } else if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                log.info("节点被删除：" + event.getPath());
                // 处理节点删除的逻辑
                // ...
            }
            // 这里可以添加更多事件类型的处理逻辑
        });

        // 建立与ZooKeeper的连接
        if (zooKeeper.getState() == ZooKeeper.States.CONNECTING) {
            log.info("正在连接到ZooKeeper...");
            while (zooKeeper.getState() == ZooKeeper.States.CONNECTING) {
                TimeUnit.SECONDS.sleep(100);
            }
        }

        if (zooKeeper.getState() == ZooKeeper.States.CONNECTED) {
            log.info("已连接到ZooKeeper");

            // 注册需要监听的路径
            String pathToWatch = "/your/config/path";
            zooKeeper.exists(pathToWatch, true); // 监听路径的变化

            // 或者使用getData方法监听节点的数据变化
            // zooKeeper.getData(pathToWatch, true); // 监听节点数据的变化

            // 初始化数据
            byte[] initialConfigData = zooKeeper.getData(pathToWatch, false, null);
            // 处理初始配置数据
            // ...
        }else{
            log.error("连接到ZooKeeper失败");
            throw new Exception("连接到ZooKeeper失败");
        }

        // todo 初始化，与zk建立连接，注册监听路径，当配置有变化随时更新
    }
}
