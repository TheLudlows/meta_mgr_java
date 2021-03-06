package com.huawei.hwcloud.gaussdb.data.store.race;

import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.huawei.hwcloud.gaussdb.data.store.race.Counter.*;
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.*;
import static java.lang.System.exit;


public class DataStoreRaceImpl implements DataStoreRace {
    private DBEngine dbEngine;
    private ThreadLocal<long[]> tempLong = ThreadLocal.withInitial(() -> new long[64]);

    @Override
    public boolean init(String dir) {
        try {
            CacheService.init();
            LOG("Init dir:" + dir);
            dbEngine = new WALEngine(dir);
            dbEngine.init();
            return true;
        } catch (Throwable e) {
            LOG_ERR("init ", e);
        }
        return false;
    }

    @Override
    public void deInit() {
        try {
            LOG("all write:" + writeCounter.sum());
            LOG("all read:" + readCounter.sum());
            LOG(mem());
            LOG("random read:" + randomRead.sum());
            LOG("cache read:" + cacheHit.sum());
            LOG("total read size:" + totalReadSize.sum() / 1024 / 1024 + "M");

            dbEngine.print();
            dbEngine.close();
        } catch (Throwable e) {
            LOG_ERR("deInit ", e);
        }
    }

    @Override
    public void writeDeltaPacket(DeltaPacket deltaPacket) {
        try {
            writeCounter.add(1);
            long count = deltaPacket.getDeltaCount();
            long v = deltaPacket.getVersion();
            List<DeltaPacket.DeltaItem> list = deltaPacket.getDeltaItem();
            Map<Long, List<DeltaPacket.DeltaItem>> map = new HashMap();
            for (int i = 0; i < count; i++) {
                long k = list.get(i).getKey();
                List<DeltaPacket.DeltaItem> exist = map.get(k);
                if (exist == null) {
                    exist = new ArrayList<>(3);
                    map.put(k, exist);
                }
                exist.add(list.get(i));
            }
            long[] sum = tempLong.get();
            for (List<DeltaPacket.DeltaItem> items : map.values()) {
                DeltaPacket.DeltaItem first = items.get(0);
                byte[] exceed = new byte[16];
                if (items.size() > 1) {
                    for (int i = 0; i < 64; i++) {
                        sum[i] = 0;
                    }
                    for (DeltaPacket.DeltaItem item : items) {
                        for (int i = 0; i < 64; i++) {
                            sum[i] += item.getDelta()[i];
                        }
                    }
                    for (int i = 0; i < 64; i++) {
                        if (sum[i] > Integer.MAX_VALUE) {
                            int mutiple = (int) (sum[i] / Integer.MAX_VALUE);
                            first.getDelta()[i] = (int) (sum[i] % Integer.MAX_VALUE);
                            exceed[i / 4] |= mutiple << (6 - i % 4 * 2);
                        } else if (sum[i] < Integer.MIN_VALUE) {
                            int mutiple = (int) (sum[i] / Integer.MIN_VALUE);
                            first.getDelta()[i] = (int) (sum[i] % Integer.MIN_VALUE);
                            exceed[i / 4] |= mutiple << (6 - i % 4 * 2);
                        } else {
                            first.getDelta()[i] = (int) sum[i];
                        }
                    }
                }
                dbEngine.write(v, first, exceed);
            }

        } catch (Throwable e) {
            LOG_ERR("writeDeltaPacket ", e);
            exit(1);
        }
    }

    @Override
    public Data readDataByVersion(long key, long version) {
        try {
            readCounter.add(1);
            Data data = dbEngine.read(key, version);
            return data;
        } catch (Throwable e) {
            LOG_ERR("readDataByVersion ", e);
            exit(1);
        }
        return null;
    }
}
