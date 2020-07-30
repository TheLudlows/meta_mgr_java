package com.huawei.hwcloud.gaussdb.data.store.race;

import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.LOG;
import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.LOG_ERR;


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
            dbEngine = new MappedEngine(dir);
            dbEngine.init();
            return true;
        } catch (Exception e) {
            LOG_ERR("init ", e);
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
            LOG_ERR("deInit ", e);
        }
    }

    @Override
    public void writeDeltaPacket(DeltaPacket deltaPacket) {
        try {
            counter.getAndIncrement();
            long count = deltaPacket.getDeltaCount();
            long v = deltaPacket.getVersion();
            List<DeltaPacket.DeltaItem> list = deltaPacket.getDeltaItem();
            for (int i = 0; i < count; i++) {
                DeltaPacket.DeltaItem item = list.get(i);
                dbEngine.write(v, item);
            }
        } catch (Exception e) {
            LOG_ERR("writeDeltaPacket ", e);
        }
    }

    @Override
    public Data readDataByVersion(long key, long version) {
        try {
            readCounter.add(1);
            Data data = dbEngine.read(key, version);
            if (data == null) {
                LOG("empty data key" + key + " v " + version);
            }
            return data;
        } catch (Exception e) {
            LOG_ERR("readDataByVersion ", e);
        }
        return null;
    }
}
