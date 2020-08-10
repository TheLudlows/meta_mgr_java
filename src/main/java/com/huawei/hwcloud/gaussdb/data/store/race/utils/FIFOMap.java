package com.huawei.hwcloud.gaussdb.data.store.race.utils;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.huawei.hwcloud.gaussdb.data.store.race.Versions;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.wal_size;

public class FIFOMap {
    int index;
    long[] arr;
    LongObjectHashMap<Versions> map;

    public FIFOMap() {
        map = new LongObjectHashMap<>();
        arr = new long[wal_size];
    }

    public void put(long key, int v, int off) {
        Versions versions = map.get(key);
        if(versions == null) {
        }
    }
}
