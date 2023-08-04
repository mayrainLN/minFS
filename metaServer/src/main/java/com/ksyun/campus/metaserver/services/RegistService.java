package com.ksyun.campus.metaserver.services;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class RegistService implements ApplicationRunner {
    public void registToCenter(){
        // todo 将本实例信息注册至zk中心，包含信息 ip、port
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        registToCenter();
    }
}
