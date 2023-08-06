package com.ksyun.campus.metaserver.util;

import cn.hutool.json.JSONUtil;

import dto.DataServerInstance;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * @author :MayRain
 * @version :1.0
 * @date :2023/8/6 21:47
 * @description :
 */
@Component
@Slf4j
public class LoadBalanceUtil {
    @Resource
    private CuratorFramework client;

    /**
     * 选出3个dataServer，返回DataServerInstance
     * 暂时只是根据剩余容量来选择
     * @return
     */
    public List<DataServerInstance> writeLoadBalance() {
        String basePath = "/MinFS/dataServerInfo";
        List<String> dataServerList = null;
        try {
            dataServerList = client.getChildren().forPath(basePath);
        } catch (Exception e) {
            log.error("获取dataServer列表信息失败,路径为:{}", basePath);
            e.printStackTrace();
        }
        //暂时负载策略是选出三个剩余容量最大的
        PriorityQueue<DataServerInstance> priorityQueue = new PriorityQueue(3,
                (x,y) -> {
                    DataServerInstance xx = (DataServerInstance) x;
                    DataServerInstance yy = (DataServerInstance) x;
                    return xx.getCapacity()<yy.getCapacity()?1:-1;
            }
        );
        for (String child : dataServerList) {
            String childPath = basePath + "/" + child;
            String jsonStr = null;
            try {
                Stat stat = new Stat();
                byte[] data = client.getData().storingStatIn(stat).forPath(childPath);
                jsonStr = new String(data);
            } catch (Exception e) {
                log.error("获取dataServer信息失败,路径为:{}", childPath);
                e.printStackTrace();
            }
            log.info("dataServerInfo:{}", jsonStr);
            DataServerInstance instance = JSONUtil.toBean(jsonStr, DataServerInstance.class);
            if (priorityQueue.size() < 3) {
                priorityQueue.add(instance);
            } else {
                DataServerInstance peek = priorityQueue.peek();
                if (peek.getCapacity() < instance.getCapacity()) {
                    priorityQueue.poll();
                    priorityQueue.add(instance);
                }
            }
        }
        return new ArrayList<>(priorityQueue);
    }
}
