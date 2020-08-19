package com.huawei.hwcloud.gaussdb.data.store.race;

import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;

import java.nio.ByteBuffer;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.item_size;

public class VersionCache {
    static final int max_cache_size = item_size * 8;
    int maxMatchIndex;
    long key;
    ByteBuffer buffer;
    Data data;

    public VersionCache() {
        data = new Data(64);
        // 4kb
        buffer = ByteBuffer.allocate(max_cache_size);
    }

}
