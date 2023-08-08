package com.ksyun.campus.client;

import com.ksyun.campus.client.util.HttpClientUtil;
import com.ksyun.campus.client.util.ZkUtil;
import lombok.SneakyThrows;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.mime.ByteArrayBody;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpResponse;

import java.io.IOException;
import java.io.OutputStream;

public class FSOutputStream extends OutputStream {
    private String fileLogicPath;
    private String fileSystem = "default";

    public FSOutputStream( String fileSystem,String fileLogicPath) {
        this.fileLogicPath = fileLogicPath;
        this.fileSystem = fileSystem;
    }

    public FSOutputStream(String fileLogicPath) {
        this.fileLogicPath = fileLogicPath;
    }

    /**
     * 参考父类，写入一个字节，高于8为的会被忽略
     * @param b
     * @throws IOException
     */
    @Override
    public void write(int b) throws IOException {

    }

    @SneakyThrows
    @Override
    public void write(byte[] b) throws IOException {
        String metaServerMasterAddr = ZkUtil.getMetaServerMasterAddr();
        if(!fileSystem.startsWith("/")){
            fileSystem = "/" + fileSystem;
        }
        if(!fileLogicPath.startsWith("/")){
            fileLogicPath = "/" + fileLogicPath;
        }
        String fullLogicPath = fileSystem + fileLogicPath;
        HttpClient httpClient = HttpClientUtil.defaultClient();
        HttpPost httpPost = new HttpPost("http://"+metaServerMasterAddr + "/create");
        httpPost.setHeader("Content-Type", "multipart/form-data");
        // 构建 MultipartEntityBuilder，用于创建 formdata 格式的请求体
        MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create();
        entityBuilder.addTextBody("path", fullLogicPath, ContentType.TEXT_PLAIN);
        ByteArrayBody byteArrayBody = new ByteArrayBody(b, ContentType.APPLICATION_OCTET_STREAM, "tempFileName");
        entityBuilder.addPart("file", byteArrayBody);

        entityBuilder.setBoundary("----WebKitFormBoundary7MA4YWxkTrZu0gW");
        // 构建请求实体
        HttpEntity httpEntity = entityBuilder.build();
        httpPost.setEntity(httpEntity);
        httpPost.setHeader("Content-Type", "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW");
        HttpResponse response = httpClient.execute(httpPost);
        if(response.getCode() != 200){
            throw new RuntimeException("write file failed");
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        super.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
