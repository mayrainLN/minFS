package com.ksyun.campus.client;

import com.ksyun.campus.client.domain.StatInfo;

import java.io.IOException;
import java.io.InputStream;

public class FSInputStream extends InputStream {
    private StatInfo statInfo;
    @Override
    public int read() throws IOException {
        return 0;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return super.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return super.read(b, off, len);
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

    public void setStatInfo(StatInfo statInfo) {
        this.statInfo = statInfo;
    }
}
