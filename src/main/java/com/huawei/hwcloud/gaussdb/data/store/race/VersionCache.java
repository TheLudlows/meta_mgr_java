package com.huawei.hwcloud.gaussdb.data.store.race;

import java.nio.ByteBuffer;

public class VersionCache {
    static final int default_size = 4;
    static final int max_cache_size = 64 * 8 * 8 * 8;
    long key;
    int vs[];
    int size;
    ByteBuffer buffer;

    public VersionCache() {
        vs = new int[default_size];
        // 4kb
        buffer = ByteBuffer.allocateDirect(max_cache_size);
    }

    public void add(int v) {
        int len = vs.length;
        if (len == size) {
            len = size + 2;
            int[] temp = new int[len];
            System.arraycopy(vs, 0, temp, 0, size);
            vs = temp;
        }
        vs[size++] = v;
    }
}
