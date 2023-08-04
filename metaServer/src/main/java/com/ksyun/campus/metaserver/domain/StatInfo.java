package com.ksyun.campus.metaserver.domain;

import lombok.Data;
import java.util.List;

@Data
public class StatInfo {
    public String path;
    public long size; // 文件大小
    public long mtime; // 最近修改时间
    public FileType type;
    // 内容数据的三副本索引
    private List<ReplicaData> replicaData; // 文件Data存储索引,因为存在多个DataServer中，所以是List
    public StatInfo() {}
}
