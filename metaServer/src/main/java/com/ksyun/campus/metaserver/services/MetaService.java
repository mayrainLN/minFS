package com.ksyun.campus.metaserver.services;

import org.springframework.stereotype.Service;

@Service
public class MetaService {
    public Object pickDataServer(){
        // todo 通过zk内注册的ds列表，选择出来一个ds，用来后续的wirte
        // 需要考虑选择ds的策略？负载
        return null;
    }
}
