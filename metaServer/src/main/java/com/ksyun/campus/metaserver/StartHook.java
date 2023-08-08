package com.ksyun.campus.metaserver;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * @author :MayRain
 * @version :1.0
 * @date :2023/8/6 18:41
 * @description :
 */
@Slf4j
@Component
public class StartHook implements ApplicationRunner {
    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("MetaServer Hook运行...");
        // TODO 启动时注册，主从结构
    }
}
