package com.conf.parse.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipInputStream;

public class XmlZipInputStream extends InputStream {
    ZipInputStream zin;

    public XmlZipInputStream(ZipInputStream zin) {
        this.zin = zin;
    }

    @Override
    public int hashCode() {
        return zin.hashCode();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return zin.read(b);
    }

    @Override
    public int read() throws IOException {
        return zin.read();
    }

    @Override
    public boolean equals(Object obj) {
        return zin.equals(obj);
    }

    @Override
    public int available() throws IOException {
        return zin.available();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return zin.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return zin.skip(n);
    }

    @Override
    public boolean markSupported() {
        return zin.markSupported();
    }

    @Override
    public void mark(int readlimit) {
        zin.mark(readlimit);
    }

    @Override
    public void close() throws IOException {
        // 手工关闭
    }

    @Override
    public void reset() throws IOException {
        zin.reset();
    }

    @Override
    public String toString() {
        return zin.toString();
    }

}
