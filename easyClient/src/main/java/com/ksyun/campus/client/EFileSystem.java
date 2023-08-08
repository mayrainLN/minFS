package com.ksyun.campus.client;

import cn.hutool.json.JSONUtil;
import com.ksyun.campus.client.domain.ClusterInfo;
import com.ksyun.campus.client.domain.StatInfo;
import dto.PrefixConstants;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.ArrayList;
import java.util.List;

@Slf4j
// 文件相关的操作，都是以path开头
public class EFileSystem extends FileSystem {

    private String fileName = "default";

    public EFileSystem() {
    }

    public EFileSystem(String fileName) {
        this.fileName = fileName;
    }

    // 暂时只写path。包含fileSystem
    public FSInputStream open(String path) {
        return null;
    }

    public FSOutputStream create(String path) {
        return null;
    }

    public boolean mkdir(String path) {
        return false;
    }

    public boolean delete(String path) {
        return false;
    }

    public StatInfo getFileStats(String path) {
        return null;
    }

    public List<StatInfo> listFileStats(String path) {
        return null;
    }

    static CuratorFramework client;

    static {
        String zkConnectionString = "localhost:2181"; // 替换成实际的ZooKeeper连接字符串

        client = CuratorFrameworkFactory.newClient(
                zkConnectionString, new ExponentialBackoffRetry(1000, 3)
        );

        client.start();
    }
    /**
     * 老师原话：这个接口一定要实现出来
     * @return
     */
    @SneakyThrows
    public ClusterInfo getClusterInfo() {
        ClusterInfo clusterInfo = new ClusterInfo();
        List<ClusterInfo.DataServerMsg> dataServerMsgList = new ArrayList<>();
        List<String> children = client.getChildren().forPath(PrefixConstants.ZK_PATH_DATA_SERVER_INFO);

        for (String child : children) {
            String childPath = PrefixConstants.ZK_PATH_DATA_SERVER_INFO + "/" + child;
            byte[] childData = client.getData().forPath(childPath);
            String childDataString = new String(childData);
            dataServerMsgList.add(JSONUtil.toBean(childDataString, ClusterInfo.DataServerMsg.class));
        }
        clusterInfo.setDataServer(dataServerMsgList);

        List<String> childNodeName = client.getChildren().forPath(PrefixConstants.ZK_PATH_META_SERVER_INFO);
        if(childNodeName.size() == 0){
            log.error("没有元数据服务器");
            return clusterInfo;
        }else if(childNodeName.size() == 1){
            String childPath = PrefixConstants.ZK_PATH_META_SERVER_INFO + "/" + childNodeName.get(0);
            byte[] childData = client.getData().forPath(childPath);
            String childDataString = new String(childData);
            ClusterInfo.MetaServerMsg dataServerMsg = JSONUtil.toBean(childDataString, ClusterInfo.MetaServerMsg.class);
            clusterInfo.setMasterMetaServer(dataServerMsg);
        }else if (childNodeName.size() == 2){
            // 两个节点，谁的序号小，谁就是主节点
            String masterNodeName,slaveNodeName;
            if(childNodeName.get(0).compareTo(childNodeName.get(1)) < 0) {
                masterNodeName = childNodeName.get(0);
                slaveNodeName = childNodeName.get(1);
            }else{
                masterNodeName = childNodeName.get(1);
                slaveNodeName = childNodeName.get(0);
            }

            String masterPath = PrefixConstants.ZK_PATH_META_SERVER_INFO + "/" + masterNodeName;
            byte[] masterData = client.getData().forPath(masterPath);
            String masterDataString = new String(masterData);
            ClusterInfo.MetaServerMsg masterDataServerMsg = JSONUtil.toBean(masterDataString, ClusterInfo.MetaServerMsg.class);
            clusterInfo.setMasterMetaServer(masterDataServerMsg);

            String slavePath = PrefixConstants.ZK_PATH_META_SERVER_INFO + "/" + slaveNodeName;
            byte[] slaveData = client.getData().forPath(slavePath);
            String slaveDataString = new String(slaveData);
            ClusterInfo.MetaServerMsg slaveDataServerMsg = JSONUtil.toBean(slaveDataString, ClusterInfo.MetaServerMsg.class);
            clusterInfo.setSlaveMetaServer(slaveDataServerMsg);

        }

        return clusterInfo;
    }
}
