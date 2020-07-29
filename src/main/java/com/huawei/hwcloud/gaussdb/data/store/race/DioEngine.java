package com.huawei.hwcloud.gaussdb.data.store.race;

import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

public class DioEngine implements DBEngine {
    @Override
    public void init() {

    }

    @Override
    public void write(long v, DeltaPacket.DeltaItem item) {

    }

    @Override
    public void print() {

    }

    @Override
    public Data read(long key, long v) {
        return null;
    }
}
