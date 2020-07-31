package test;

import com.huawei.hwcloud.gaussdb.data.store.race.DataStoreRace;
import com.huawei.hwcloud.gaussdb.data.store.race.DataStoreRaceImpl;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.util.ArrayList;
import java.util.List;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.LOG;

/**
 * 模拟本地测试
 */
public class FakeTest {
    public static void main(String[] args) throws InterruptedException {
        DataStoreRace store = new DataStoreRaceImpl();
        store.init("data");
        Thread t1 = new Thread(() -> write(store, 0, 10));
        Thread t2 = new Thread(() -> write(store, 10, 20));

        t1.start();
        t2.start();
        t1.join();
        t2.join();
        for (int i = 0; i < 1000; i++) {
            Data data = store.readDataByVersion(i, /*ThreadLocalRandom.current().nextInt(9999)*/8);
            LOG(data.toString());
        }
        store.deInit();
    }

    public static long[] randomDelta() {
        long[] longs = new long[64];
        for (int i = 0; i < 64; i++) {
            longs[i] = 1;
        }
        return longs;
    }

    public static void write(DataStoreRace dataStoreRace, int vs, int ve) {
        for (int i = 0; i < 200000; i++) {
            for (int j = vs; j < ve; j++) {
                DeltaPacket deltaPacket = new DeltaPacket();
                deltaPacket.setDeltaCount(2L);
                deltaPacket.setVersion(j);
                List list = new ArrayList<>();
                DeltaPacket.DeltaItem item = new DeltaPacket.DeltaItem();
                item.setKey(i);
                item.setDelta(randomDelta());
                list.add(item);
                item = new DeltaPacket.DeltaItem();
                item.setKey(i);
                item.setDelta(randomDelta());
                list.add(item);
                deltaPacket.setDeltaItem(list);
                dataStoreRace.writeDeltaPacket(deltaPacket);
            }
        }
    }
}
