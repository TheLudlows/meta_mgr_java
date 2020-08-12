package com.huawei.hwcloud.gaussdb.data.store.race;

import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.io.File;
import java.io.IOException;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.BUCKET_SIZE;
import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.MONITOR_TIME;
import static com.huawei.hwcloud.gaussdb.data.store.race.Counter.*;
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.*;

/**
 * key - version :mapped file
 * fields :wal -> channel write
 */
public class WALEngine implements DBEngine {
    private String dir;
    private WALBucket buckets[];
    // 数据监控线程
    private Thread backPrint;

    public WALEngine(String dir) {
        this.dir = dir + "/";
        File f = new File(dir);
        if (!f.exists()) {
            f.mkdir();
        }
        buckets = new WALBucket[BUCKET_SIZE];
    }

    @Override
    public void init() {
        for (int i = 0; i < BUCKET_SIZE; i++) {
            buckets[i] = new WALBucket(dir + i, i);
        }
        // 后台监控线程
        backPrint = new Thread(() ->
        {
            try {
                long lastWrite = 0;
                long lastRead = 0;
                long lastMergeRead = 0;
                long lastRandomRead = 0;
                long lastReadSize = 0;
                while (true) {
                    long read = readCounter.sum();
                    long write = writeCounter.sum();
                    long mr = mergeRead.sum();
                    long rr = randomRead.sum();
                    long rs = totalReadSize.sum();
                    LOG("[LAST" + MONITOR_TIME + "ms],[Read " + (read - lastRead) + "],[Write " + (write - lastWrite)
                            + "],[MergeRead " + (mr - lastMergeRead) + "],[RandomRead " + (rr - lastRandomRead) + "],[ReadSize "
                            + ((rs - lastReadSize) / 1024 / 1024) + "M]" + ",[CPU " + cpuLoad() + "]"
                    );

                    LOG(mem());
                    lastRead = read;
                    lastWrite = write;
                    lastMergeRead = mr;
                    lastRandomRead = rr;
                    lastReadSize = rs;
                    Thread.sleep(MONITOR_TIME);
                }
            } catch (InterruptedException e) {
                LOG_ERR("err", e);
            }
        });
        backPrint.setDaemon(true);
        backPrint.start();
    }

    @Override
    public void write(long v, DeltaPacket.DeltaItem item) throws IOException {
        buckets[index(item.getKey())].write(v, item);
    }

    @Override
    public void print() {
        for (WALBucket b : buckets) {
            b.print();
        }
    }

    @Override
    public Data read(long key, long v) throws IOException {
        return buckets[index(key)].read(key, v);
    }

    @Override
    public void close() {
    }
}