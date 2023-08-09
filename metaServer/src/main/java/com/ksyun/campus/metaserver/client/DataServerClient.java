package com.ksyun.campus.metaserver.client;

import com.google.common.collect.Maps;
import com.ksyun.campus.metaserver.util.HttpUtil;
import dto.DataServerInstance;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author :MayRain
 * @version :1.0
 * @date :2023/8/7 11:47
 * @description : 主要是封装网络请求，提供给Service层调用。方便Service优化，比如CountDownLatch等
 */
@Component
@Slf4j
public class DataServerClient {
    @Autowired
    RestTemplate restTemplate;

    @Resource
    HttpUtil httpUtil;

    public ResponseEntity appendReplicas(DataServerInstance dataServerInstance,
                                         String path,
                                         MultipartFile file){
        byte[] bytes = null;
        try {
            bytes = file.getBytes();
        } catch (IOException e) {
            log.error("文件内容读取错误");
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.BAD_REQUEST);
        }
        String url = "http://" + dataServerInstance.getHost() + ":" + dataServerInstance.getPort() + "/write";

        Map<String, Object> map = new HashMap<>();
        map.put("path", path);
        map.put("file", bytes);
        ResponseEntity<String> stringResponseEntity = httpUtil.sendPostToDataServer(url, map);
        return stringResponseEntity;
    }

    public ResponseEntity createFileOnDataServer(DataServerInstance dataServerInstance,
                                         String path){
        String url = "http://" + dataServerInstance.getHost() + ":" + dataServerInstance.getPort() + "/create";
        Map<String, Object> map = new HashMap<>();
        map.put("path", path);
        ResponseEntity<String> stringResponseEntity = httpUtil.sendPostToDataServer(url, map);
        return stringResponseEntity;
    }

    public ResponseEntity deleteFile(DataServerInstance dataServerInstance,
                                     String path){

        String url = "http://" + dataServerInstance.getHost() + ":" + dataServerInstance.getPort() + "/delete";
        Map map = new HashMap();
        map.put("path", path);

        return httpUtil.sendPostToDataServer(url, map);
    }

}
