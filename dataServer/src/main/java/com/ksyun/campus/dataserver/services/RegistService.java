package com.ksyun.campus.dataserver.services;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class RegistService implements ApplicationRunner {

    @Override
    public void run(ApplicationArguments args) throws Exception {
        registToCenter();
    }

    public void registToCenter() {
        // todo 将本实例信息注册至zk中心，包含信息 ip、port、capacity、rack、zone
    }

    public List<Map<String, Integer>> getDslist() {
        return null;
    }


}
