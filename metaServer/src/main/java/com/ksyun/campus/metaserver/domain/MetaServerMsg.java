package com.ksyun.campus.metaserver.domain;

/**
 * @author :MayRain
 * @version :1.0
 * @date :2023/8/8 15:36
 * @description :
 */
public class MetaServerMsg{
    private String host;
    private int port;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
