package com.ksyun.campus.metaserver.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.List;


/**
 * 文件元数据信息
 */
@Data
@Builder
@Accessors(chain = true)
@NoArgsConstructor
@AllArgsConstructor
public class StatInfo {
    /**
     *  文件逻辑路径
     */
    public String path;

    public long size;

    public long mtime;

    public boolean isCommitted;

    /**
     * volum 、file、directory
     */
    public FileType type;

    /**
     * 文件Data存储索引,因为存在多个DataServer中，所以是List
     * 内容数据的三副本索引
     */
    public List<ReplicaData> replicaData;

    @Override
    public String toString() {
        return "StatInfo{" +
                "path='" + path + '\'' + ','+ '\n'+
                "size=" + size +','+  '\n'+
                "mtime=" + mtime +','+ '\n'+
                "type=" + type +','+ '\n'+
                "replicaData=" + replicaData +'\n'+
                '}';
    }
}



