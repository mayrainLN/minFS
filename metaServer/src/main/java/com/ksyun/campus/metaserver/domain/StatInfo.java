package com.ksyun.campus.metaserver.domain;

import lombok.Data;
import java.util.List;


/**
 * 文件元数据信息
 */
@Data
public class StatInfo {
    /**
     *  文件逻辑路径
     */
    public String path;

    public long size;

    public long mtime;

    /**
     * volum 、file、directory
     */
    public FileType type;

    /**
     * 文件Data存储索引,因为存在多个DataServer中，所以是List
     * 内容数据的三副本索引
     */
    private List<ReplicaData> replicaData;

    public StatInfo() {}
}



