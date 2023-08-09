package com.ksyun.campus.client;

import cn.hutool.json.JSONUtil;
import com.ksyun.campus.client.domain.ClusterInfo;
import com.ksyun.campus.client.domain.StatInfo;
import com.ksyun.campus.client.util.HttpClientConfig;
import com.ksyun.campus.client.util.HttpClientUtil;
import com.ksyun.campus.client.util.MetaServerClient;
import com.ksyun.campus.client.util.ZkUtil;
import dto.PrefixConstants;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.mime.ByteArrayBody;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpResponse;

import java.util.*;

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

    // 创建文件的时候，会把path和fileSystem修正
    // TODO 需求理解错了。老师在这里的意思是create只是创建文件
    // TODO 后续是调用write方法追加数据，调用close方法完成Commit
    public FSOutputStream create(String path) {
        if(!fileName.startsWith("/")){
            fileName = "/" + fileName;
        }
        if(!path.startsWith("/")){
            path = "/" + path;
        }
        Map<String, Object> formDatas = new HashMap<>();
        formDatas.put("path", fileName + path);
        HttpResponse res = HttpClientUtil.sendPostToMetaServer("/create",formDatas);
        if(res.getCode()!=200){
            throw new RuntimeException("创建文件失败");
        }
        // 构造函数中会修正path和fileSystem
        return new FSOutputStream(fileName, path);
    }

    public boolean mkdir(String path) {
        return false;
    }

    public boolean delete(String path) {
        if(!fileName.startsWith("/")){
            fileName = "/" + fileName;
        }
        if(!path.startsWith("/")){
            path = "/" + path;
        }
        Map<String, Object> formDatas = new HashMap<>();
        formDatas.put("path", fileName + path);
        HttpResponse httpResponse = HttpClientUtil.sendPostToMetaServer("/delete", formDatas);
        return httpResponse.getCode() == 200;
    }

    public StatInfo getFileStats(String path) {
        return null;
    }

    public List<StatInfo> listFileStats(String path) {
        return null;
    }
    
    /**
     * 老师原话：这个接口一定要实现出来
     * @return
     */
    @SneakyThrows
    public ClusterInfo getClusterInfo() {
        ClusterInfo clusterInfo = new ClusterInfo();
        List<ClusterInfo.DataServerMsg> dataServerMsgList = new ArrayList<>();
        List<String> children = ZkUtil.client().getChildren().forPath(PrefixConstants.ZK_PATH_DATA_SERVER_INFO);

        for (String child : children) {
            String childPath = PrefixConstants.ZK_PATH_DATA_SERVER_INFO + "/" + child;
            byte[] childData = ZkUtil.client().getData().forPath(childPath);
            String childDataString = new String(childData);
            dataServerMsgList.add(JSONUtil.toBean(childDataString, ClusterInfo.DataServerMsg.class));
        }
        clusterInfo.setDataServer(dataServerMsgList);

        List<String> childNodeName = ZkUtil.client().getChildren().forPath(PrefixConstants.ZK_PATH_META_SERVER_INFO);
        if(childNodeName.size() == 0){
            log.error("没有元数据服务器");
            return clusterInfo;
        }else if(childNodeName.size() == 1){
            String childPath = PrefixConstants.ZK_PATH_META_SERVER_INFO + "/" + childNodeName.get(0);
            byte[] childData = ZkUtil.client().getData().forPath(childPath);
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
            byte[] masterData = ZkUtil.client().getData().forPath(masterPath);
            String masterDataString = new String(masterData);
            ClusterInfo.MetaServerMsg masterDataServerMsg = JSONUtil.toBean(masterDataString, ClusterInfo.MetaServerMsg.class);
            clusterInfo.setMasterMetaServer(masterDataServerMsg);

            String slavePath = PrefixConstants.ZK_PATH_META_SERVER_INFO + "/" + slaveNodeName;
            byte[] slaveData = ZkUtil.client().getData().forPath(slavePath);
            String slaveDataString = new String(slaveData);
            ClusterInfo.MetaServerMsg slaveDataServerMsg = JSONUtil.toBean(slaveDataString, ClusterInfo.MetaServerMsg.class);
            clusterInfo.setSlaveMetaServer(slaveDataServerMsg);

        }

        return clusterInfo;
    }
}
