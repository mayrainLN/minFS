package com.ksyun.campus.client.util;

import lombok.SneakyThrows;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.entity.mime.ByteArrayBody;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import org.apache.hc.client5.http.socket.PlainConnectionSocketFactory;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.config.Registry;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;

import java.lang.reflect.Method;
import java.util.Map;

public class HttpClientUtil {
    private static HttpClient httpClient;
    public static HttpClient createHttpClient(HttpClientConfig config) {

        int socketSendBufferSizeHint = config.getSocketSendBufferSizeHint();
        int socketReceiveBufferSizeHint = config.getSocketReceiveBufferSizeHint();
        int buffersize = 0;
        if (socketSendBufferSizeHint > 0 || socketReceiveBufferSizeHint > 0) {
            buffersize = Math.max(socketSendBufferSizeHint, socketReceiveBufferSizeHint);
        }
        SocketConfig soConfig = SocketConfig.custom()
                .setTcpNoDelay(true).setSndBufSize(buffersize)
                .setSoTimeout(Timeout.ofMilliseconds(config.getSocketTimeOut()))
                .build();
        ConnectionConfig coConfig = ConnectionConfig.custom()
                .setConnectTimeout(Timeout.ofMilliseconds(config.getConnectionTimeOut()))
                .build();
        RequestConfig reConfig;
        RequestConfig.Builder builder= RequestConfig.custom()
                .setConnectTimeout(Timeout.ofMilliseconds(config.getConnectionTimeOut()))
                .setResponseTimeout(Timeout.ofMilliseconds(config.getSocketTimeOut()))
                ;
        reConfig=builder.build();
        PlainConnectionSocketFactory sf = PlainConnectionSocketFactory.getSocketFactory();
        Registry<ConnectionSocketFactory> r = RegistryBuilder.<ConnectionSocketFactory> create().register("http", sf).build();
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(r);
        connectionManager.setMaxTotal(config.getMaxConnections());
        connectionManager.setDefaultMaxPerRoute(connectionManager.getMaxTotal());
        connectionManager.setDefaultConnectionConfig(coConfig);
        connectionManager.setDefaultSocketConfig(soConfig);


        httpClient = HttpClients.custom().setConnectionManager(connectionManager).setRetryStrategy(new DefaultHttpRequestRetryStrategy(config.getMaxRetry(), TimeValue.ZERO_MILLISECONDS))
                .setDefaultRequestConfig(reConfig)
                .build();
        return httpClient;
    }

    public static HttpClient defaultClient(){
        return createHttpClient(HttpClientConfig.defaultConfig());
    }

    /**
     *
     * @param url 不包含http前缀、host和端口
     * @param formDatas 其中的path应当是完整的逻辑路径(包含fileSystem)
     * @return
     */
    @SneakyThrows
    public static HttpResponse sendPost(String url, Map<String,Object> formDatas){
        String metaServerMasterAddr = ZkUtil.getMetaServerMasterAddr();
        HttpClient httpClient = HttpClientUtil.defaultClient();
        HttpPost httpPost = new HttpPost("http://"+metaServerMasterAddr+url);
        httpPost.setHeader("Content-Type", "multipart/form-data");
        // 构建 MultipartEntityBuilder，用于创建 formdata 格式的请求体
        MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create();
        Object path = formDatas.get("path");
        if(path!= null){
            entityBuilder.addTextBody("path", (String) path, ContentType.TEXT_PLAIN);
        }
        Object file = formDatas.get("file");
        if(path!= null){
            ByteArrayBody byteArrayBody = new ByteArrayBody((byte[]) file, ContentType.APPLICATION_OCTET_STREAM, "tempFileName");
            entityBuilder.addPart("file", byteArrayBody);
        }

        entityBuilder.setBoundary(HttpClientConfig.HTTP_FORMDATA_BOUNDARY);
        // 构建请求实体
        HttpEntity httpEntity = entityBuilder.build();
        httpPost.setEntity(httpEntity);
        httpPost.setHeader("Content-Type", "multipart/form-data; boundary="+HttpClientConfig.HTTP_FORMDATA_BOUNDARY);
        return httpClient.execute(httpPost);
    }
}
