package com.ksyun.campus.metaserver.domain;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class ReplicaData {
    //
    public String id;
    public String dsNode; //机器信息，如localhost:8001
    /**
     * 真实的文件位置
     * 比如test.txt, 逻辑位置为 /test.txt
     * 那我们就存储为 /data/replica1/test.txt
     */
    public String path;
}
