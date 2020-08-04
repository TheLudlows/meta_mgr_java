package com.huawei.hwcloud.gaussdb.data.store.race;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.*;
import static com.huawei.hwcloud.gaussdb.data.store.race.DataStoreRaceImpl.readCounter;
import static com.huawei.hwcloud.gaussdb.data.store.race.DataStoreRaceImpl.writeCounter;
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.*;
import static java.nio.file.StandardOpenOption.*;

/**
 * key - version :mapped file
 * fields :wal -> channel write
 */
public class WALEngine implements DBEngine {
    private String dir;
    private WALBucket buckets[];
    private volatile boolean stop = false;
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
            buckets[i] = new WALBucket(dir + i);
        }
        // 后台监控线程
        backPrint = new Thread(() ->
        {
            try {
                StringBuffer buffer = new StringBuffer();
                long lastWrite = 0;
                long lastRead = 0;
                while (!stop) {
                    // buckets info
                   /* buffer.setLength(0);
                    for (WALBucket bucket : buckets) {
                        buffer.append(bucket.dir + " " + bucket.count + " " + bucket.index.size() + "|");
                    }
                    LOG(buffer.toString());*/
                    // request
                    long read = readCounter.sum();
                    long write = writeCounter.sum();
                    LOG("Last" + MONITOR_TIME + "ms,read:" + (read - lastRead) + ",write:" + (write - lastWrite));
                    LOG(mem());
                    lastRead = read;
                    lastWrite = write;
                    Thread.sleep(MONITOR_TIME);
                }
            } catch (InterruptedException e) {
                LOG_ERR("err", e);
            }
        });
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
        this.stop = true;
        try {
            backPrint.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}