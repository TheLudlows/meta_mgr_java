package com.huawei.hwcloud.gaussdb.data.store.race;

import java.nio.ByteBuffer;

public class BucketCache {
    long[] keys;
    int windowLen;
    int curWindow;
    ByteBuffer[] buffers;
    int index;

    public BucketCache(int windowLen) {
        this.keys = new long[100000];
        this.windowLen = windowLen;
        this.buffers = new  ByteBuffer[windowLen];
        for(int i=0;i<windowLen;i++) {
            buffers[i] = ByteBuffer.allocateDirect(64*8*8*8);
        }
    }
    public void addKey(long key) {
        keys[index++] = key;
    }

    public int indexOfKey(long key) {
        int start=curWindow*windowLen;
        int end = start+windowLen;
        for(int i=start;i<end;i++) {
            if(keys[i] == key) {
                return i;
            }
        }
        return -1;
    }

}
