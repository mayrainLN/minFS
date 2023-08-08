package com.ksyun.campus.metaserver.client;

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
import java.io.IOException;
import java.net.URI;
import java.util.Objects;

/**
 * @author :MayRain
 * @version :1.0
 * @date :2023/8/7 11:47
 * @description :
 */
@Component
@Slf4j
public class DataServerClient {
    @Autowired
    RestTemplate restTemplate;

    /**
     * 写文件到指定的DataServer
     */
    public ResponseEntity writeFileToDataServer(DataServerInstance dataServerInstance,
                                  String fileSystem,
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

        MultiValueMap<String, Object> formData = new LinkedMultiValueMap<>();
        ByteArrayResource fileResource = new ByteArrayResource(bytes) {
            @Override
            public String getFilename() {
                return Objects.requireNonNull(file.getOriginalFilename());
            }
        };
        formData.add("file", fileResource);
        formData.add("path", path);

        HttpHeaders headers = new HttpHeaders();

        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
        headers.set("fileSystem", fileSystem);

        String targetUrl = "http://" + dataServerInstance.getIp() + ":" + dataServerInstance.getPort() + "/write";
        RequestEntity<MultiValueMap<String, Object>> requestEntity = RequestEntity
                .post(URI.create(targetUrl))
                .headers(headers)
                .body(formData);

        // 发送formData格式的请求
        ResponseEntity<String> responseEntity = restTemplate.exchange(
                requestEntity,
                String.class
        );
        return responseEntity;
    }

    public ResponseEntity deleteFile(DataServerInstance dataServerInstance,
                                     String fileSystem,
                                     String path){

        MultiValueMap<String, Object> formData = new LinkedMultiValueMap<>();
        formData.add("path", path);

        HttpHeaders headers = new HttpHeaders();

        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
        headers.set("fileSystem", fileSystem);

        String targetUrl = "http://" + dataServerInstance.getIp() + ":" + dataServerInstance.getPort() + "/delete";
        RequestEntity<MultiValueMap<String, Object>> requestEntity = RequestEntity
                .post(URI.create(targetUrl))
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
