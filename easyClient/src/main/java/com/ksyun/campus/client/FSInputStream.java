package com.ksyun.campus.client;

import com.ksyun.campus.client.domain.FileType;
import com.ksyun.campus.client.domain.ReplicaData;
import com.ksyun.campus.client.domain.StatInfo;
import com.ksyun.campus.client.util.HttpClientUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

@Slf4j
public class FSInputStream extends InputStream {
    private StatInfo statInfo;

    /**
     * 从输入流中读取下一个字节的数据。
     * 返回值byte为int型，取值范围为0到255。如果由于到达流的末尾而没有可用的字节，则返回值-1
     * @return
     * @throws IOException
     */
    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        read(b);
        return b[0];
    }

    /**
     * 从输入流中读取一定数量的字节并将其存储到缓冲区数组b中。实际读取的字节数以整数形式返回。
     * @param b
     * @return
     * @throws IOException
     */
    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if(!isReadable(statInfo)){
            return -1;
        }
        String replicaAddr = getReplicaAddr(statInfo);
        String fullUrl = "http://" + replicaAddr + "/read";
        Map<String,Object> form = new HashMap<>();
        form.put("path", statInfo.getPath());
        form.put("offset", off);
        form.put("length", len);
        ClassicHttpResponse classicHttpResponse = HttpClientUtil.sendPot(fullUrl, form);
        if(classicHttpResponse.getCode() != 200){
            log.error("读取文件失败");
            return -1;
        }
        HttpEntity body = classicHttpResponse.getEntity();
        if(body.getContentLength()==0){
            log.error("文件读取完毕");
            return -1;
        }
        if(b.length-off< body.getContentLength()){
            log.error("缓冲区不足");
            return -1;
        }
        return body.getContent().read(b, off, len);
    }

    @Override
    public void close() throws IOException {
        setStatInfo(null);
    }

    public void setStatInfo(StatInfo statInfo) {
        this.statInfo = statInfo;
    }

    private boolean isReadable(StatInfo statInfo){
        if(statInfo.getType() != FileType.File){
            return false;
        }
        if(statInfo.getReplicaData().size() == 0){
            return false;
        }
        if(!statInfo.isCommitted()){
            return false;
        }
        return true;
    }

    private String getReplicaAddr(StatInfo statInfo){
        List<ReplicaData> replicaData = statInfo.getReplicaData();
        String replicaAddr = replicaData.get(new Random().nextInt(replicaData.size())).dsNode;
        return replicaAddr;

    }
}
