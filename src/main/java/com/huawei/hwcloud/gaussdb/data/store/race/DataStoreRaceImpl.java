package com.huawei.hwcloud.gaussdb.data.store.race;

import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class DataStoreRaceImpl implements DataStoreRace {

    private AtomicInteger counter;
    private static final Object o = new Object();
    private ConcurrentHashMap<Long, Object> key;
    private LongAdder readCounter;
    private DBEngine dbEngine;

    @Override
    public boolean init(String dir) {
        this.counter = new AtomicInteger();
        this.key = new ConcurrentHashMap<>();
        readCounter = new LongAdder();
        System.out.println("Init dir:" + dir);
        dbEngine = new DBEngine(dir);
        dbEngine.init();
        return true;
    }

    @Override
    public void deInit() {
        System.out.println("all request:" + counter.get());
        System.out.println("all keys:" + key.size());
        System.out.println("all read:" + readCounter.sum());
        dbEngine.print();
    }

    @Override
    public void writeDeltaPacket(DeltaPacket deltaPacket) {
        counter.getAndIncrement();
        long v = deltaPacket.getVersion();
        for (DeltaPacket.DeltaItem item : deltaPacket.getDeltaItem()) {
            if (!key.contains(item.getKey())) {
                key.put(item.getKey(), o);
            }
            dbEngine.write(v, item);
        }
    }

    @Override
    public Data readDataByVersion(long key, long version) {
        readCounter.add(1);
        return dbEngine.read(key, version);
    }
}
