package com.ksyun.campus.metaserver.domain;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class ReplicaData {
    public String id;
    public String dsNode;
    public String path;
}
