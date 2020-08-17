package com.huawei.hwcloud.gaussdb.data.store.race;

import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.huawei.hwcloud.gaussdb.data.store.race.Counter.*;
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.*;
import static java.lang.System.exit;


public class DataStoreRaceImpl implements DataStoreRace {
    private DBEngine dbEngine;

    @Override
    public boolean init(String dir) {
        try {
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
            LOG("all match:" + allMatchTimes.sum());
            LOG("random read:" + randomRead.sum());
            LOG("merge read:" + mergeRead.sum());
            LOG("total read size:" + totalReadSize.sum()/1024/1024 + "M");

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
            short count = deltaPacket.getDeltaCount();
            long v = deltaPacket.getVersion();
            List<DeltaPacket.DeltaItem> list = deltaPacket.getDeltaItem();
            Map<Long, DeltaPacket.DeltaItem> map = new HashMap<>();
            for (int i = 0; i < count; i++) {
                long k = list.get(i).getKey();
                DeltaPacket.DeltaItem item = list.get(i);
                DeltaPacket.DeltaItem inMapItem;
                if ((inMapItem = map.get(k)) == null) {
                    map.put(k, item);
                } else {
                    inMapItem.add(item.getDelta());
                }
            }
            for (DeltaPacket.DeltaItem item : map.values()) {
                dbEngine.write(v, item);
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
