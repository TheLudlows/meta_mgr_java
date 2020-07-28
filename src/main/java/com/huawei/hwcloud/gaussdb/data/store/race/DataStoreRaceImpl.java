package com.huawei.hwcloud.gaussdb.data.store.race;

import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class DataStoreRaceImpl implements DataStoreRace {

    private AtomicInteger counter;
    private static final Object o = new  Object();
    private ConcurrentHashMap<Long,Object> key;
    private LongAdder readCounter;

    @Override
    public boolean init(String dir) {
        this.counter = new AtomicInteger();
        this.key = new ConcurrentHashMap<>();
        readCounter = new LongAdder();
        return true;
    }

    @Override
    public void deInit() {
        System.out.println("all request" + counter.get());
        System.out.println("all keys:" + key.toString());
        System.out.println("all read:" + readCounter.sum());
    }

    @Override
    public void writeDeltaPacket(DeltaPacket deltaPacket) {
        int n = counter.getAndIncrement();
        for(DeltaPacket.DeltaItem item : deltaPacket.getDeltaItem()) {
            if(!key.contains(item.getKey())) {
                key.put(item.getKey(), o);
            }
        }
        if(n < 1000) {
            System.out.println(Thread.currentThread().getName() +" " + deltaPacket.toString());
        }
    }

    @Override
    public Data readDataByVersion(long key, long version) {
        // Delete the following line and write your code here.
        readCounter.add(1);
        return new Data();
    }
}
