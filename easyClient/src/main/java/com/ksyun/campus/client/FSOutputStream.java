package com.ksyun.campus.client;

import java.io.IOException;
import java.io.OutputStream;

public class FSOutputStream extends OutputStream {
    @Override
    public void write(int b) throws IOException {

    }

    @Override
    public void write(byte[] b) throws IOException {
        super.write(b);
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
