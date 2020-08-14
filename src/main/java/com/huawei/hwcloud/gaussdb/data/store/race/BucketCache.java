package com.huawei.hwcloud.gaussdb.data.store.race;

import java.nio.ByteBuffer;

public class BucketCache {
    public static final int WINDOWS_LEN = 8;
    long[] keys;
    int windowLen;
    int curWindow;
    ByteBuffer[] buffers;
    int size;

    public BucketCache(int windowLen) {
        this.keys = new long[100000];
        this.windowLen = windowLen;
        this.buffers = new ByteBuffer[windowLen];
        for (int i = 0; i < windowLen; i++) {
            buffers[i] = ByteBuffer.allocateDirect(64 * 8 * 8 * 8);
        }
        curWindow = -1;
    }

    public void addKey(long key) {
        keys[size++] = key;
    }

    public boolean inCache(int i) {
        int start = curWindow * windowLen;
        return i >= start && i < start + windowLen;
    }

    public ByteBuffer getBuf(int i) {
        return buffers[i%windowLen];
    }

    public int start(int i) {
        return i/windowLen*windowLen;
    }

    public int end(int i) {
         int end = i/windowLen*windowLen + windowLen ;
         if(end>size) {
             end = size;
         }
         return end;
    }

    public void setCur(int index) {
        this.curWindow = index/windowLen;
    }
}
