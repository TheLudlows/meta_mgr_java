package com.huawei.hwcloud.gaussdb.data.store.race;

import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.util.ArrayList;
import java.util.List;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.LOG;

/**
 * 模拟本地测试
 */
public class FakeTest {
    public static void main(String[] args) {
        DataStoreRace store = new DataStoreRaceImpl();
        store.init("data/");

        for(int i=0;i<1000;i++) {
            for(int j=0;j<10;j++) {
                DeltaPacket deltaPacket = new DeltaPacket();
                deltaPacket.setVersion(j);
                deltaPacket.setDeltaCount(10);
                List list = new ArrayList<>();
                DeltaPacket.DeltaItem item = new DeltaPacket.DeltaItem();
                item.setKey(i);
                item.setDelta(randomDelta());
                list.add(item);
                deltaPacket.setDeltaItem(list);
                store.writeDeltaPacket(deltaPacket);
            }

        }
        for(int i= 0;i<1000;i++ ) {
            Data data = store.readDataByVersion(i, /*ThreadLocalRandom.current().nextInt(9999)*/8);
            //LOG(data.toString());
        }
        store.deInit();
    }

    public static long[] randomDelta() {
        long[] longs = new long[64];
        for(int i=0;i<64;i++) {
            longs[i] = 1;
        }
        return longs;
    }
}
