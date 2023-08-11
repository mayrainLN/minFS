package com.ksyun.campus.metaserver.db;

import com.ksyun.campus.metaserver.domain.ReplicaData;
import dto.DataServerInstance;
import dto.PrefixConstants;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @author :MayRain
 * @version :1.0
 * @date :2023/8/10 19:18
 * @description : 本类作为元信息的存储层
 * 问题是之前的结构是按照zk存储元信息写的，改动存储方式要改很多地方
 * 先确定能上线，有时间再改动吧
 */
@Deprecated
@Slf4j
public class Database {
    public static FileTree fileTree;
    private static void init(){
        fileTree = FileTree.initDir(PrefixConstants.DB_BASE_PATH_META_DATA);
        fileTree.setPath(PrefixConstants.DB_BASE_PATH_META_DATA);
    }

    static {
        init();
    }

    // 创建文件，如果目录不存在，需要创建目录

    /**
     * @param path 文件逻辑路径
     * @param realPathMap ds实例->文件实际路径
     * @return
     */
    public static boolean create(String path, Map<DataServerInstance, String> realPathMap){
        path = PrefixConstants.DB_BASE_PATH_META_DATA + path;
        String[] partPath = path.split("/"); // 第一个是/metaData
        FileTree parent = fileTree;
        StringBuilder currFullPath = new StringBuilder(PrefixConstants.DB_BASE_PATH_META_DATA);
        for (int i = 1 ; i < partPath.length; i++) {
            String currName = partPath[i];
            FileTree curr = parent.getChildren().get(currName);
            currFullPath = currFullPath.append("/").append(currName);
            if(i == partPath.length - 1){
                // 创建文件
                curr = FileTree.initFile(currName);
                curr.setPath(currFullPath.toString());
                List<ReplicaData> ReplicaDatas= new ArrayList<>();
                for (Map.Entry<DataServerInstance, String> entry : realPathMap.entrySet()) {
                    ReplicaData replicaData = new ReplicaData();
                    replicaData.setId(UUID.randomUUID().toString());
                    String dsAddr = entry.getKey().getHost()+":"+entry.getKey().getPort();
                    replicaData.setDsNode(dsAddr);
                    replicaData.setPath(entry.getValue());
                    ReplicaDatas.add(replicaData);
                }
                curr.setReplicaData(ReplicaDatas);
                parent.getChildren().put(currName,curr);
                return true;
            }

            if(curr == null){
                // 父目录不存在，创建父目录
                curr = FileTree.initDir(currName);
                curr.setPath(currFullPath.toString());
                parent.getChildren().put(currName,curr);
                parent = curr;
            }else{
                parent = curr;
            }
        }
        return false;
    }

    /**
     * @param path 文件逻辑路径
     * @return
     */
    public static boolean mkdir(String path){
        path = PrefixConstants.DB_BASE_PATH_META_DATA + path;
        String[] partPath = path.split("/"); // 第一个是/metaData
        FileTree parent = fileTree;
        StringBuilder currFullPath = new StringBuilder(PrefixConstants.DB_BASE_PATH_META_DATA);
        for (int i = 1 ; i < partPath.length; i++) {
            String currName = partPath[i];
            FileTree curr = parent.getChildren().get(currName);
            currFullPath = currFullPath.append("/").append(currName);
            if(i == partPath.length - 1){
                // 创建文件
                curr = FileTree.initDir(currName);
                curr.setPath(currFullPath.toString());
                parent.getChildren().put(currName,curr);
                return true;
            }

            if(curr == null){
                // 父目录不存在，创建父目录
                curr = FileTree.initDir(currName);
                curr.setPath(currFullPath.toString());
                parent.getChildren().put(currName,curr);
                parent = curr;
            }else{
                parent = curr;
            }
        }
        return false;
    }

    private static FileTree getFile(String path){
        path = PrefixConstants.DB_BASE_PATH_META_DATA + path;
        String[] partPath = path.split("/"); // 第一个是/metaData
        FileTree parent = fileTree;
        FileTree curr = null;
        for (int i = 1 ; i < partPath.length; i++) {
            String currName = partPath[i];
            curr = parent.getChildren().get(currName);
            if(curr == null){
                log.error("文件不存在");
                return null;
            }
            parent = curr;
        }
        return curr;
    }

    // 创建文件，如果目录不存在，需要创建目录
    public static boolean commit(String path){
        FileTree curr = getFile(path);
        if(curr == null){
            return false;
        }
        curr.setCommitted(true);
        curr.setMtime(System.currentTimeMillis());
        return true;
    }

    public void syncSlave(String ops,String fileLogicPath){

    }
}
