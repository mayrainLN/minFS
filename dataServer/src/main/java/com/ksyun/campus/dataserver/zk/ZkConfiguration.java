package com.ksyun.campus.dataserver.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author :MayRain
 * @version :1.0
 * @date :2023/8/5 16:04
 * @description :
 */
@Configuration
public class ZkConfiguration {

    @Autowired
    private CuratorConf curatorConf;

    @Value("${zookeeper.addr}")
    private String zkAddr;
    /**
     * 这里会自动调用一次start，后续不需要重复调用
     */
    @Bean(initMethod = "start")
    public CuratorFramework curatorFramework() {
        return CuratorFrameworkFactory.newClient(
                zkAddr,
                curatorConf.getSessionTimeoutMs(),
                curatorConf.getConnectionTimeoutMs(),
                new RetryNTimes(curatorConf.getRetryCount(), curatorConf.getElapsedTimeMs()));
    }
}
