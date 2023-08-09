package com.ksyun.campus.metaserver.util;

import cn.hutool.http.ContentType;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import sun.net.www.http.HttpClient;

import javax.annotation.Resource;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

/**
 * @author :MayRain
 * @version :1.0
 * @date :2023/8/9 19:16
 * @description :
 */
@Component
public class HttpUtil {
    @Resource
    private RestTemplate restTemplate;

    /**
     * 约定发送请求前将FileSystem提前转换，所以这里只需要关注FormData里的参数
     * @param url 全路径，需要包含协议头、端口号、路径
     * @param formDatasMap
     * @return
     */
    public ResponseEntity<String> sendPostToDataServer(String url, Map<String, Object> formDatasMap){
        MultiValueMap<String, Object> formData = new LinkedMultiValueMap<>();
        byte[] file = (byte[]) formDatasMap.get("file");
        String path = (String) formDatasMap.get("path");
        if(path != null){
            formData.add("path", path);
        }
        if (file != null) {
            ByteArrayResource fileResource = new ByteArrayResource(file) {
                @Override
                public String getFilename() {
                    return "temFileName";
                }
            };
            formData.add("file", fileResource);
        }
        HttpHeaders headers = new HttpHeaders();

        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        RequestEntity<MultiValueMap<String, Object>> requestEntity = RequestEntity
                .post(URI.create(url))
                .headers(headers)
                .body(formData);

        // 发送formData格式的请求
        ResponseEntity<String> responseEntity = restTemplate.exchange(
                requestEntity,
                String.class
        );
        return responseEntity;
    }
}
