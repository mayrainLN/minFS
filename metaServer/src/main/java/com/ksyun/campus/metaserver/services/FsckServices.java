package com.ksyun.campus.metaserver.services;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class FsckServices {

    //@Scheduled(cron = "0 0 0 * * ?") // 每天 0 点执行
    @Scheduled(fixedRate = 30*60*1000) // 每隔 30 分钟执行一次
    public void fsckTask() {
        // todo 全量扫描文件列表
        // todo 检查文件副本数量是否正常
        // todo 更新文件副本数：3副本、2副本、单副本
        // todo 记录当前检查结果
    }
}
