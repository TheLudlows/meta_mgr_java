package com.huawei.hwcloud.gaussdb.data.store.race;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.huawei.hwcloud.gaussdb.data.store.race.utils.Tuple2;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.*;
import static java.nio.file.StandardOpenOption.*;

public class MappedEngine implements DBEngine {
    private String dir;
    private Bucket buckets[];

    public MappedEngine(String dir) {
        this.dir = dir + "/";
        buckets = new Bucket[BUCKET_SIZE];
    }

    public void init() {
        for (int i = 0; i < BUCKET_SIZE; i++) {
            buckets[i] = new Bucket(dir + i);
        }
    }

    public void write(long v, DeltaPacket.DeltaItem item) {
        buckets[index(item.getKey())].write(v, item);
    }

    public Data read(long key, long v) {
        return buckets[index(key)].read(key, v);
    }

    public void print() {
        for (Bucket b : buckets) {
            b.print();
        }
    }

    private int index(long key) {
        return (int) Math.abs((key ^ (key >>> 32)) % BUCKET_SIZE);
    }
}

class Bucket {
    private LongObjectHashMap<List<Tuple2<Long, Integer>>> index;
    private MappedByteBuffer dataBuffer;
    private MappedByteBuffer counterBuffer;
    private MappedByteBuffer keyBuffer;
    private int count;
    private String fileName;

    public Bucket(String fileName) {
        try {
            this.fileName = fileName;
            /**
             * 450 key % 32
             */
            index = new LongObjectHashMap(1024*16*8);
            String counterFileName = fileName + ".counter";
            String dataFileName = fileName + ".data";
            String keyFileName = fileName + ".key";
            counterBuffer = FileChannel.open(new File(counterFileName).toPath(), CREATE, READ, WRITE)
                    .map(FileChannel.MapMode.READ_WRITE, 0, 4);
            dataBuffer = FileChannel.open(new File(dataFileName).toPath(), CREATE, READ, WRITE)
                    .map(FileChannel.MapMode.READ_WRITE, 0, FILED_MAPPED_SIZE);
            keyBuffer = FileChannel.open(new File(keyFileName).toPath(), CREATE, READ, WRITE)
                    .map(FileChannel.MapMode.READ_WRITE, 0, FILED_MAPPED_SIZE / 32);
            tryRecovery();
        } catch (IOException e) {
            LOG(e.getMessage());
        }
    }

    private void tryRecovery() {
        int count = counterBuffer.getInt(0);
        if (count != 0) {
            this.count = count;
            //keyBuffer.position(count * 16);
            dataBuffer.position(count * 64 * 8);
            //恢复索引
            for (int i = 0; i < count; i++) {
                long k = keyBuffer.getLong();
                long v = keyBuffer.getLong();
                List<Tuple2<Long, Integer>> versions = index.get(k);
                if (versions == null) {
                    versions = new ArrayList<>();
                    index.put(k, versions);
                }
                versions.add(new Tuple2<>(v, i));
            }
        }
        LOG("recover from " + fileName + " count:" + count + " index size:" + index.size());
    }

    public synchronized void write(long v, DeltaPacket.DeltaItem item) {
        long key = item.getKey();
        keyBuffer.putLong(key);
        keyBuffer.putLong(v);
        for (long l : item.getDelta()) {
            dataBuffer.putLong(l);
        }
        List<Tuple2<Long, Integer>> data = index.get(key);
        if (data == null) {
            data = new ArrayList();
            index.put(key, data);
        }
        data.add(new Tuple2<>(v, count));
        counterBuffer.putInt(0, ++count);
    }

    public synchronized Data read(long k, long v) {
        List<Tuple2<Long, Integer>> versions = index.get(k);
        if (versions == null) {
            return null;
        }
        Data data = new Data(k, v);

        long[] fields = new long[64];
        for (Tuple2<Long, Integer> t : versions) {
            if (t.a <= v) {
                long[] delta = getFiled(t.b);
                for (int i = 0; i < delta.length; i++) {
                    fields[i] += delta[i];
                }
            }
        }
        data.setField(fields);
        return data;
    }

    /**
     * 读取文件中的field
     */
    private long[] getFiled(Integer n) {
        long[] filed = new long[64];
        for (int i = 0; i < 64; i++) {
            filed[i] = dataBuffer.getLong(n * 64 * 8 + i * 8);
        }
        return filed;
    }

    public void print() {
        LOG(fileName + " count:" + count);
    }
}

