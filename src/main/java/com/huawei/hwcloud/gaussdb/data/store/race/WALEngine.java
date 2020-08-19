package com.huawei.hwcloud.gaussdb.data.store.race;

import com.carrotsearch.hppc.LongByteHashMap;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

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
    private AtomicInteger bIndex=new AtomicInteger(-1);
    private ThreadLocal<Byte> BUCKETINDEX=ThreadLocal.withInitial(()->(byte)bIndex.incrementAndGet());
    public static final LongByteHashMap keyBucketMap=new LongByteHashMap(4600001,0.99);
    private byte[] lock=new byte[0];

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
                long lastHitCache = 0;
                long lastRandomRead = 0;
                long lastReadSize = 0;
                while (true) {
                    long read = readCounter.sum();
                    long write = writeCounter.sum();
                    long hit = cacheHit.sum();
                    long rr = randomRead.sum();
                    long rs = totalReadSize.sum();
                    int indexSize=0;
                    for(WALBucket bucket:buckets){
                        indexSize+=bucket.index.size();
                    }
                    LOG("[LAST" + MONITOR_TIME + "ms],[Read " + (read - lastRead) + "],[Write " + (write - lastWrite)
                            + "],[hit " + (hit - lastHitCache) + "],[RandomRead " + (rr - lastRandomRead) + "],[ReadSize "
                            + ((rs - lastReadSize) / 1024 / 1024) + "M],[totalIndex "+indexSize+"]"
                    );
                    LOG(mem());
                    lastRead = read;
                    lastWrite = write;
                    lastHitCache = hit;
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
        try {
            System.gc();
            Thread.sleep(5000);
        }catch (Exception e) {
            LOG("err");
        }
    }

    @Override
    public void write(long v, DeltaPacket.DeltaItem item,byte[] exceed) throws IOException {
        byte idx=keyBucketMap.getOrDefault(item.getKey(),(byte)-1);
        if(idx==-1){
            synchronized (lock){
                idx=keyBucketMap.getOrDefault(item.getKey(),(byte)-1);
                if(idx==-1){
                    idx=(byte)(BUCKETINDEX.get()%30);
                    keyBucketMap.put(item.getKey(),idx);
                }
            }
        }
        buckets[idx].write(v, item,exceed);
    }

    @Override
    public void print() {
        for (WALBucket b : buckets) {
            b.print();
        }
    }

    @Override
    public Data read(long key, long v) throws IOException {
        Byte idx=keyBucketMap.get(key);
        if(idx==null){
            return null;
        }
        return buckets[idx].read(key, v);
    }

    @Override
    public void close() {
    }
}