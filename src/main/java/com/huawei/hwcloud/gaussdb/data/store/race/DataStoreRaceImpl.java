package com.huawei.hwcloud.gaussdb.data.store.race;

import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.LOG;


public class DataStoreRaceImpl implements DataStoreRace {

    private AtomicInteger counter;
    private static final Object o = new Object();
    private ConcurrentHashMap<Long, Object> key;
    private LongAdder readCounter;
    private DBEngine dbEngine;

    @Override
    public boolean init(String dir) {
        try {

            this.counter = new AtomicInteger();
            this.key = new ConcurrentHashMap<>();
            readCounter = new LongAdder();
            LOG("Init dir:" + dir);
            dbEngine = new DBEngine(dir);
            dbEngine.init();
            return true;
        } catch (Exception e) {
            LOG(e.getMessage());
        }
        return false;
    }

    @Override
    public void deInit() {
        try {
            LOG("all request:" + counter.get());
            LOG("all keys:" + key.size());
            LOG("all read:" + readCounter.sum());
            dbEngine.print();
        } catch (Exception e) {
            LOG(e.getMessage());
        }
    }

    @Override
    public void writeDeltaPacket(DeltaPacket deltaPacket) {
        try {
            counter.getAndIncrement();
            long v = deltaPacket.getVersion();
            for (DeltaPacket.DeltaItem item : deltaPacket.getDeltaItem()) {
                if (!key.contains(item.getKey())) {
                    key.put(item.getKey(), o);
                }
                dbEngine.write(v, item);
            }
        } catch (Exception e) {
            LOG(e.getMessage());
        }
    }

    @Override
    public Data readDataByVersion(long key, long version) {
        try {
            readCounter.add(1);
            return dbEngine.read(key, version);
        } catch (Exception e) {
            LOG(e.getMessage());
        }
        return null;
    }
}
