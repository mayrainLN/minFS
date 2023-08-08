package com.ksyun.campus.metaserver.domain;

import dto.DataServerInstance;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class ReplicaData {
    public String id;
    public String dsNode; //机器信息，如localhost:8001
    /**
     * 真实的文件位置
     * 比如test.txt, 逻辑位置为 /test.txt
     * 那我们就存储为 /data/10.1.2.116-9000/test.txt
     */
    public String path;

    public ReplicaData(DataServerInstance instance) {
        this.dsNode = instance.getHost() + ":" + instance.getPort();
    }

    public ReplicaData() {
    }

    @Override
    public String toString() {
        return "ReplicaData{" +
                "id='" + id + '\'' +"\n"+
                "dsNode='" + dsNode + '\'' +"\n"+
                "path='" + path + '\'' +"\n"+
                '}';
    }
}
