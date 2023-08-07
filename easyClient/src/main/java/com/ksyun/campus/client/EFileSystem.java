package com.ksyun.campus.client;

import com.ksyun.campus.client.domain.ClusterInfo;
import com.ksyun.campus.client.domain.StatInfo;

import java.util.List;

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

    /**
     * 老师原话：这个接口一定要实现出来
     * @return
     */
    public ClusterInfo getClusterInfo() {
        return null;
    }
}
