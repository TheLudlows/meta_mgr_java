package com.huawei.hwcloud.gaussdb.data.store.race;

import com.huawei.hwcloud.gaussdb.data.store.race.utils.Tuple2;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.*;

public class MappedEngine implements DBEngine {
    private String dir;
    private Bucket buckets[];

    public MappedEngine(String dir) {
        this.dir = dir+"/";
        buckets = new Bucket[BUCKET_SIZE];
        init();
    }

    public void init() {
        for (int i = 0; i < BUCKET_SIZE; i++) {
            buckets[i] = new Bucket(dir + i);
        }
    }

    public void write(long v, DeltaPacket.DeltaItem item) {
        int i = (int) item.getKey() % BUCKET_SIZE;
        buckets[i].write(v, item);
    }

    public Data read(long key, long v) {
        int i = (int) (key % BUCKET_SIZE);
        return buckets[i].read(key, v);
    }

    public void print() {
        for (Bucket b : buckets) {
            b.print();
        }
    }
}

class Bucket {
    private Map<Long, List<Tuple2<Long, Integer>>> index;
    private MappedByteBuffer dataBuffer;
    private MappedByteBuffer counterBuffer;
    private MappedByteBuffer keyBuffer;
    private int count;
    private String fileName;

    public Bucket(String fileName) {
        try {
            this.fileName = fileName;
            index = new HashMap<>();
            counterBuffer = FileChannel.open(new File(fileName + ".counter").toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
                    .map(FileChannel.MapMode.READ_WRITE, 0, 4);
            dataBuffer =  FileChannel.open(new File(fileName + DATA_SUFFIX).toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
                    .map(FileChannel.MapMode.READ_WRITE, 0, FILED_MAPPED_SIZE);
            keyBuffer = FileChannel.open(new File(fileName + ".key").toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
                    .map(FileChannel.MapMode.READ_WRITE, 0, FILED_MAPPED_SIZE / 32);
            tryRecovery();
        } catch (IOException e) {
            e.printStackTrace();
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
       LOG("recover index size:" + index.size());
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
        counterBuffer.putInt(0, count++);
    }

    public synchronized Data read(long k, long v) {
        Data data = new Data(k, v);
        List<Tuple2<Long, Integer>> versions = index.get(k);
        if (versions == null) {
            return data;
        }
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

