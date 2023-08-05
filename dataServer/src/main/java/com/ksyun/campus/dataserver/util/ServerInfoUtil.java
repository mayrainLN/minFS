package com.ksyun.campus.dataserver.util;

import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.InetAddress;

/**
 * @author :MayRain
 * @version :1.0
 * @date :2023/8/5 16:51
 * @description :
 */
@Component
public class ServerInfoUtil {
    @Value("${server.port}")
    private int serverPort;

    @SneakyThrows
    public String getIp() {
        return InetAddress.getLocalHost().getHostAddress();
        // 防止ipv4地址出意外，直接写本机吧
//        return "localhost";
    }

    public int getPort() {
        return serverPort;
    }
}

