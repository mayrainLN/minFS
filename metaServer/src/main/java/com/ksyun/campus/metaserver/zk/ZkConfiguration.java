package com.ksyun.campus.metaserver.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author :MayRain
 * @version :1.0
 * @date :2023/8/6 18:43
 * @description :
 */
@Configuration
public class ZkConfiguration {

    @Autowired
    private CuratorConf curatorConf;

    /**
     * 这里会自动调用一次start，后续不需要重复调用
     */
    @Bean(initMethod = "start")
    public CuratorFramework curatorFramework() {
        return CuratorFrameworkFactory.newClient(
                curatorConf.getConnectString(),
                curatorConf.getSessionTimeoutMs(),
                curatorConf.getConnectionTimeoutMs(),
                new RetryNTimes(curatorConf.getRetryCount(), curatorConf.getElapsedTimeMs()));
    }
}
