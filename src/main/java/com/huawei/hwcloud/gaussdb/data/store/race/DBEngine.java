package com.huawei.hwcloud.gaussdb.data.store.race;

import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.BUCKET_SIZE;

public class DBEngine {
    private String dir;
    private Bucket bucket[];
    public DBEngine(String dir) {
        this.dir = dir;
        bucket = new Bucket[BUCKET_SIZE];
        init();
    }

    public void init() {
        for (int i = 0; i < BUCKET_SIZE; i++) {
            bucket[i] = new Bucket(dir+"_"+i);
        }
    }

    public void write(long v, DeltaPacket.DeltaItem item) {
        int i = (int)item.getKey()%BUCKET_SIZE;
        bucket[i].write(v,item);
    }
}
