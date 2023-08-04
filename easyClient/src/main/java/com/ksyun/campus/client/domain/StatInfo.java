package com.ksyun.campus.client.domain;

import lombok.Data;

import java.util.List;

@Data
public class StatInfo {
    public String path;
    public long size;
    public long mtime;
    public FileType type;
    public List<ReplicaData> replicaData;

    public StatInfo() {
    }

    @Override
    public String toString() {
        return "StatInfo{" +
                "path='" + path + '\'' +
                ", size=" + size + '\'' +
                ", mtime=" + mtime + '\'' +
                ", type=" + type + '\'' +
                ", replicaData=" + replicaData + '\'' +
                '}';
    }
}
