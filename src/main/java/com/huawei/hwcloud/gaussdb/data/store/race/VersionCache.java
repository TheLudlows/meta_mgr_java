package com.huawei.hwcloud.gaussdb.data.store.race;

import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;

import java.nio.ByteBuffer;

public class VersionCache {
    static final int max_cache_size = 64 * 4 * 8 * 8;
    long key;
    ByteBuffer buffer;
    Data data;

    public VersionCache() {
        data = new Data(64);
        // 4kb
        buffer = ByteBuffer.allocateDirect(max_cache_size);
    }

}
