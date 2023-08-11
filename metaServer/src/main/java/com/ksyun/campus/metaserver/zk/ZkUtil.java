package com.ksyun.campus.metaserver.zk;

import cn.hutool.json.JSONUtil;
import dto.PrefixConstants;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.Watcher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Deprecated
/**
 * 本类用于MetaServer选举，以及主从切换
 * 目前的项目结构暂时不不适合用MetaServer存储元信息，感觉改不完了，后续有时间再采取这种方式
 */
public class ZkUtil {
    private static CuratorFramework client;
    public static final String ZK_PATH_META_SERVER_INFO = "/MinFS/metaServerInfo";

    public static volatile boolean isMaster = false;

    public static volatile String masterAddr ;

    public static volatile String slaveAddr ;

    static {
        int heartbeatTimeoutMs = 30000; // 30秒
        initZkClient(heartbeatTimeoutMs);
        init();
        addListener();
    }

    public static CuratorFramework client() {
        return client;
    }

    /**
     * 1. 初始化主从路由表、主从标志位。根据目录下的节点数量即可确定当前是主还是从
     * 2. 如果本机是从节点，添加监听器，监听主节点的健康情况，主节点挂到后自动完成切换
     * @return
     */
    @SneakyThrows
    private static void init() {
        List<String> nodeNameList = client.getChildren().forPath(ZK_PATH_META_SERVER_INFO);
        Collections.sort(nodeNameList);
        String masterNode = null, slaveNode = null;
        if(nodeNameList.isEmpty()){
            log.error("无可用MetaServer");
            throw new RuntimeException("无可用MetaServer");
        }
        if(nodeNameList.size() == 1){
            log.info("当前MetaServer Master为：" + nodeNameList.get(0));
            masterNode = nodeNameList.get(0);
            byte[] bytes = client.getData().forPath(ZK_PATH_META_SERVER_INFO + "/" + masterNode);
            Map<String, String> masterMeta = JSONUtil.toBean(new String(bytes), HashMap.class);
            masterAddr = masterMeta.get("host")+":"+masterMeta.get("port");
            // 更新本机主从标志位
            isMaster = true;
        }
        if(nodeNameList.size() == 2){
            log.info("当前MetaServer Slave为：" + nodeNameList.get(1));
            String slaveNodeName = nodeNameList.get(1);
            byte[] bytes = client.getData().forPath(ZK_PATH_META_SERVER_INFO + "/" + slaveNodeName);
            Map<String, String> slaveMeta = JSONUtil.toBean(new String(bytes), HashMap.class);
            masterAddr = slaveMeta.get("host")+":"+slaveMeta.get("port");
        }
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

    @SneakyThrows
    private static void addListener(){
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, ZK_PATH_META_SERVER_INFO, true);
        pathChildrenCache.start();
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    case CHILD_ADDED:
                        log.info("新增从节点");
                        HashMap hashMap = JSONUtil.toBean(new String(event.getData().getData()), HashMap.class);
                        slaveAddr = hashMap.get("host")+":"+hashMap.get("port");
                        break;
                    case CHILD_REMOVED:
                        if(!isMaster) {
                            log.info("从节点监听到：主节点下线");
                            log.info("开始故障转移");
                            isMaster = true;
                            masterAddr = slaveAddr;
                            slaveAddr = null;
                            log.info("本节点切换为主节点，开始对外提供服务");
                        }else{
                            // 当前是主节点，从节点下线
                            log.info("主节点监听到：从节点下线");
                            slaveAddr = null;
                        }
                        break;
                    default:
                        break;
                }
            }
        });

        // 阻塞，保持监听状态
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 本函数只能监听主节点下线，已经废弃。
     * 由addListener替代功能
     * @param masterNodeName
     */
    @Deprecated
    private static void addMasterListener(String masterNodeName) throws Exception {
        client.getData().usingWatcher((Watcher) event -> {
            try {
                // 节点断开后
                if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                    // 在这里处理节点删除后的逻辑，例如重新选举等
                    // 重新获取剩余节点列表
                    List<String> children;
                    children = client.getChildren().forPath(PrefixConstants.ZK_PATH_META_SERVER_INFO);
                    Collections.sort(children);
                    // 获取新的最小节点
                    if (!children.isEmpty()) {
                        String newMinNodePath = PrefixConstants.ZK_PATH_META_SERVER_INFO + "/" + children.get(0);
                        JSONUtil.toBean(new String(client.getData().forPath(newMinNodePath)), HashMap.class);


                        /**
                         * 在这里更新本地节点值
                         */
                        log.info("监听到MetaServer Master掉线");
                        log.info("已经切换新的MetaServer Master为：" + newMinNodePath);
                        // 启动新主节点节点的删除监听器
                        addMasterListener(newMinNodePath);
                    } else {
                        log.error("无可用MetaServer");
                        throw new RuntimeException("无可用MetaServer");
                    }
                }
            } catch (Exception e) {
                log.info("主节点断开，选举新的主节点失败");
                e.printStackTrace();
                throw new RuntimeException("主节点断开，选举新的主节点失败");
            }
        }).forPath(masterNodeName);
    }
}
