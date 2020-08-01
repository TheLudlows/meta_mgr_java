package test;

import com.huawei.hwcloud.gaussdb.data.store.race.DataStoreRace;
import com.huawei.hwcloud.gaussdb.data.store.race.DataStoreRaceImpl;
import com.huawei.hwcloud.gaussdb.data.store.race.utils.Util;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.util.ArrayList;
import java.util.List;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.LOG;

/**
 * 模拟本地测试
 */
public class FakeTest {
    static int thread_n = 30;
    static int n = 300000;

    public static void main(String[] args) throws InterruptedException {
        DataStoreRace store = new DataStoreRaceImpl();
        store.init("data");
        long start = System.currentTimeMillis();
        Thread[] ts = new Thread[thread_n];
        for (int i = 0; i < thread_n; i++) {
            int x = i;
            ts[i] = new Thread(() -> write(store, x * n, (x + 1) * n));
        }

        for (int i = 0; i < thread_n; i++) {
            ts[i].start();
        }

        for (int i = 0; i < thread_n; i++) {
            ts[i].join();
        }
        System.out.println("write over cost:" + ((System.currentTimeMillis() - start) / 1000));
        Util.printMem();
        start = System.currentTimeMillis();
        for (int i = 0; i < thread_n; i++) {
            int x = i;
            ts[i] = new Thread(() -> read(store, x * n, (x + 1) * n));
        }
        for (int i = 0; i < thread_n; i++) {
            ts[i].start();
        }

        for (int i = 0; i < thread_n; i++) {
            ts[i].join();
        }

        System.out.println("read cost: " + (System.currentTimeMillis() - start) / 1000);
        store.deInit();
    }

    private static void read(DataStoreRace store, int ks, int ke) {
        for (int i = ks; i < ke; i++) {
            Data data = store.readDataByVersion(i, /*ThreadLocalRandom.current().nextInt(9999)*/3);
            if (data != null) {
                if (data.getField()[0] != i * 4) {
                    System.out.println(i);
                }
            }
        }
    }

    public static long[] randomDelta(int d) {
        long[] longs = new long[64];
        for (int i = 0; i < 64; i++) {
            longs[i] = d;
        }
        return longs;
    }

    public static void write(DataStoreRace dataStoreRace, int ks, int ke) {
        for (int i = ks; i < ke; i++) {
            for (int j = 0; j < 5; j++) {
                DeltaPacket deltaPacket = new DeltaPacket();
                deltaPacket.setDeltaCount(1L);
                deltaPacket.setVersion(j);
                List list = new ArrayList<>();
                DeltaPacket.DeltaItem item = new DeltaPacket.DeltaItem();
                item.setKey(i);
                item.setDelta(randomDelta(i));
                list.add(item);
                deltaPacket.setDeltaItem(list);
                dataStoreRace.writeDeltaPacket(deltaPacket);
            }
        }
    }
}
