package com.ksyun.campus.client;

import com.ksyun.campus.client.util.HttpClientUtil;
import lombok.SneakyThrows;
import org.apache.hc.core5.http.HttpResponse;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public class FSOutputStream extends OutputStream {
    private String fileLogicPath;
    private String fileSystem = "default";

    public FSOutputStream( String fileSystem,String fileLogicPath) {
        if(!fileSystem.startsWith("/")){
            fileSystem = "/" + fileSystem;
        }
        if(!fileLogicPath.startsWith("/")){
            fileLogicPath = "/" + fileLogicPath;
        }

        this.fileLogicPath = fileLogicPath;
        this.fileSystem = fileSystem;
    }

    public FSOutputStream(String fileLogicPath) {
        // 规范一下路径, 减少后端特判
        if(!fileSystem.startsWith("/")){
            fileSystem = "/" + fileSystem;
        }
        if(!fileLogicPath.startsWith("/")){
            fileLogicPath = "/" + fileLogicPath;
        }
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
        Map<String,Object> map = new HashMap<>();
        // 构造函数已经修正了path和fileSystem，这里不用再修正
        map.put("path",fileSystem + fileLogicPath);
        map.put("file",b);
        HttpResponse response = HttpClientUtil.sendPostToMetaServer("/create", map);
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
