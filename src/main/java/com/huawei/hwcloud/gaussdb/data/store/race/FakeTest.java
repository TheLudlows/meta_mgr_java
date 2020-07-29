package com.huawei.hwcloud.gaussdb.data.store.race;

import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.LOG;

/**
 * 模拟本地测试
 */
public class FakeTest {
    public static void main(String[] args) {
        DataStoreRace store = new DataStoreRaceImpl();
        store.init("data/");

        for(int i=0;i<1000;i++) {
            DeltaPacket deltaPacket = new DeltaPacket();
            deltaPacket.setVersion(i);
            deltaPacket.setDeltaCount(10);
            List list = new ArrayList<>();
            for(int j=0;j<100;j++) {
                DeltaPacket.DeltaItem item = new DeltaPacket.DeltaItem();
                item.setKey(j);
                item.setDelta(randomDelta());
                list.add(item);
            }
            deltaPacket.setDeltaItem(list);
            store.writeDeltaPacket(deltaPacket);
        }
        for(int i= 0;i<100;i++ ) {
            Data data = store.readDataByVersion(i, /*ThreadLocalRandom.current().nextInt(9999)*/-1);
            LOG(data.toString());
        }
        store.deInit();
    }

    public static long[] randomDelta() {
        long[] longs = new long[64];
        for(int i=0;i<64;i++) {
            longs[i] = ThreadLocalRandom.current().nextInt(100);
        }
        return longs;
    }
}
