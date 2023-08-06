package com.ksyun.campus.client;

import org.apache.hc.client5.http.classic.HttpClient;

/**
 * fileSystem，相当于namespace，来模拟多个volume
 * 实现的时候可以按sda、sdb等方式保存，fileSystem就可以相当于你本地磁盘的 c盘、d盘、e盘等，这个维度的概念
 */
public abstract class FileSystem {
    private String fileSystem;
    private static HttpClient httpClient;

    protected void callRemote(){
//        httpClient.execute();
    }
}
