package com.ksyun.campus.client.util;

//import javax.annotation.PostConstruct;

import cn.hutool.json.JSONUtil;
import com.ksyun.campus.client.domain.ClusterInfo;
import dto.PrefixConstants;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.Collections;
import java.util.List;


@Slf4j
public class ZkUtil {
    private static CuratorFramework client;
    public static final String ZK_PATH_ROOT = "/MinFS";
    public static final String ZK_PATH_FILE_META_INFO = "/MinFS/metaData";
    public static final String ZK_PATH_DATA_SERVER_INFO = "/MinFS/dataServerInfo";
    public static final String ZK_PATH_META_SERVER_INFO = "/MinFS/metaServerInfo";

    private static volatile ClusterInfo.MetaServerMsg metaServerMsg;

    static {
        int heartbeatTimeoutMs = 30000; // 30秒
        initZkClient(heartbeatTimeoutMs);
        String masterZNodePath = initReactiveMetaServer();

        try {
            /**
             * 监听主节点，主节点挂掉后，重新获取新的主节点，并监听新的主节点
             */
            addMasterListener(masterZNodePath);
        } catch (Exception e) {
            throw new RuntimeException("监听MetaServer主节点失败");
        }
    }

    public static CuratorFramework client() {
        return client;
    }

    public static String getMetaServerMasterAddr(){
        return metaServerMsg.getHost() + ":" + metaServerMsg.getPort();
    }

    /**
     * 初始化MetaServerMsg，并返回其在ZK中的路径
     * @return
     */
    @SneakyThrows
    private static String initReactiveMetaServer() {
        List<String> nodeNameList = client.getChildren().forPath(ZK_PATH_META_SERVER_INFO);
        Collections.sort(nodeNameList);
        String masterNode = nodeNameList.get(0);
        byte[] bytes = client.getData().forPath(ZK_PATH_META_SERVER_INFO + "/" + masterNode);
        metaServerMsg = JSONUtil.toBean(new String(bytes), ClusterInfo.MetaServerMsg.class);

        return ZK_PATH_META_SERVER_INFO + "/" + masterNode;
    }

    private static void initZkClient(int heartbeatTimeoutMs) {
        // 连接zookeeper
        String zkAddr = System.getProperty("zookeeper.addr");
        client = CuratorFrameworkFactory.newClient(
                zkAddr,
                heartbeatTimeoutMs, // 设置心跳超时时间
                heartbeatTimeoutMs, // 设置连接超时时间
                new ExponentialBackoffRetry(1000, 3)
        );
        client.start();
    }

    /**
     * 监听主节点，主节点挂掉后，重新获取新的主节点，并监听新的主节点
     * @param nodePath
     */
    private static void addMasterListener(String nodePath) throws Exception{
        client.getData().usingWatcher((Watcher) event -> {
            try {
                // 节点断开后
                if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                    System.out.println("Minimum sequential node deleted: " + nodePath);
                    // 在这里处理节点删除后的逻辑，例如重新选举等
                    // 重新获取剩余节点列表
                    List<String> children;
                    children = client.getChildren().forPath(PrefixConstants.ZK_PATH_META_SERVER_INFO);
                    Collections.sort(children);
                    // 获取新的最小节点
                    if (!children.isEmpty()) {
                        String newMinNodePath = PrefixConstants.ZK_PATH_META_SERVER_INFO + "/" + children.get(0);
                        /**
                         * 在这里更新本地节点值
                         */
                        log.info("监听到MetaServer Master掉线");
                        metaServerMsg = JSONUtil.toBean(new String(client.getData().forPath(newMinNodePath)), ClusterInfo.MetaServerMsg.class);
                        log.info("已经切换新的MetaServer Master为：" + metaServerMsg.getHost() + ":" + metaServerMsg.getPort());
                        // 启动新节点的删除监听器
                        addMasterListener(newMinNodePath);
                    } else {
                        log.error("无可用MetaServer");
                        throw new RuntimeException("无可用MetaServer");
                    }
                }
            }catch (Exception e) {
                log.info("主节点断开，选举新的主节点失败");
                e.printStackTrace();
                throw new RuntimeException("主节点断开，选举新的主节点失败");
            }
        }).forPath(nodePath);
    }
}
