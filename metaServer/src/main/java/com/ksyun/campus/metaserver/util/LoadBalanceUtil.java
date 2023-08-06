package com.ksyun.campus.metaserver.util;

import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author :MayRain
 * @version :1.0
 * @date :2023/8/6 21:47
 * @description :
 */
@Component
public class LoadBalanceUtil {
    @Autowired
    private CuratorFramework client;
}
