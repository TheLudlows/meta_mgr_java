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
import static com.huawei.hwcloud.gaussdb.data.store.race.Counter.*;
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.LOG;
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.LOG_ERR;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.*;

public class WALBucket {
    public static final ThreadLocal<VersionCache> LOCAL_CACHE = ThreadLocal.withInitial(() -> new VersionCache());

    public static final ThreadLocal<ByteBuffer> LOCAL_WRITE_BUF = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(64 * 8));

    protected String dir;
    // 索引
    protected LongObjectHashMap<Versions> index;
    protected int id;
    // 文件中的位置
    private int dataPosition;
    private FileChannel fileChannel;
    private FileChannel keyChannel;
    private MappedByteBuffer keyBuf;
    private MappedByteBuffer counter;
    private int keyCounter;

    public WALBucket(String dir, int id) {
        try {
            this.id = id;
            this.dir = dir;
            // 自动扩容吧
            index = new LongObjectHashMap<>(1024 * 8 * 16);
            String dataFileName = dir + ".data";
            String keyFileName = dir + ".key";
            String countFileName = dir + ".count";

            this.fileChannel = FileChannel.open(new File(dataFileName).toPath(), CREATE, READ, WRITE);
            this.keyChannel = FileChannel.open(new File(keyFileName).toPath(), CREATE, READ, WRITE);
            dataPosition = (int) fileChannel.size();
            this.keyBuf = keyChannel.map(READ_WRITE, 0, key_mapped_size);
            this.counter = FileChannel.open(new File(countFileName).toPath(), CREATE, READ, WRITE).map(READ_WRITE, 0, 4);


            if (dataPosition % page_size != 0) {
                dataPosition = (dataPosition / page_size + 1) * page_size;
            }
            long start = System.currentTimeMillis();
            tryRecover();
            LOG("Recover " + dir + " cost:" + (System.currentTimeMillis() - start));
        } catch (Throwable e) {
            LOG_ERR("init bucket error", e);
        }
    }

    private void tryRecover() throws IOException {
        keyCounter = counter.getInt(0);
        keyBuf.position(keyCounter * 16);
        if (keyCounter == 0) {
            // 预分配，效果一般
            /*ByteBuffer buf = LOCAL_WRITE_BUF.get();
            for (int i = 0; i < 400*1024; i++) {
                buf.position(0);
                fileChannel.write(buf, dataPosition);
                dataPosition += 64 * 8;
            }
            dataPosition = 0;*/
            return;
        }
        // 恢复文件数据的索引
        ByteBuffer dataBuf = ByteBuffer.allocate(dataPosition);
        fileChannel.read(dataBuf, 0);
        for (int i = 0; i < keyCounter; i++) {
            long k = keyBuf.getLong(i * 16);
            int v = keyBuf.getInt(i * 16 + 8);
            int off = keyBuf.getInt(i * 16 + 12);
            buildIndex(k, v, off, dataBuf);
        }
    }

    private void buildIndex(long k, int v, int off, ByteBuffer dataBuf) {
        Versions versions = index.get(k);

        if (versions == null) {
            versions = new Versions(DEFAULT_SIZE);
            index.put(k, versions);
        }
        versions.add(v, off);
        long[] field = new long[64];
        for (int i = 0; i < 64; i++) {
            field[i] = dataBuf.getLong(off + i * 8);
        }
        //cache
       /* if (id < BUCKET_SIZE / cache_per) {
            versions.addField(field);
        }*/
    }

    public synchronized void write(long v, DeltaPacket.DeltaItem item) throws IOException {
        long key = item.getKey();
        ByteBuffer writeBuf = LOCAL_WRITE_BUF.get();
        Versions versions = index.get(key);
        if (versions == null) {
            versions = new Versions(DEFAULT_SIZE);
            index.put(key, versions);
        }
        int pos;
        // 计算这个version写盘的位置
        if (versions.needAlloc()) {
            pos = dataPosition;
            dataPosition += page_size;
        } else {
            int base = versions.off[versions.size / page_field_num];
            pos = base + (versions.size % page_field_num) * 64 * 8;
        }
        writeData(writeBuf, item.getDelta(), pos);
        versions.add((int) v, pos);
        writeKey(key, v, pos);
    }

    public Data read(long k, long v) throws IOException {
        Versions versions = index.get(k);
        if (versions == null) {
            return null;
        }
        if (versions.off.length > maxSize.get()) {
            maxSize.set(versions.off.length);
        }
        VersionCache cache = LOCAL_CACHE.get();
        Data data = cache.data;
        data.reset();
        data.setVersion(v);
        long[] fields = data.getField();

        if (cache.key != k) {
            data.setKey(k);
            cache.key = k;
            cache.buffer.position(0);
            int size = versions.size * 64 * 8;
            for (int i = 0; i < versions.off.length; i++) {
                randomRead.add(1);
                int limit = (i + 1) * page_size;
                cache.buffer.limit(limit > size ? size : limit);
                fileChannel.read(cache.buffer, versions.off[i]);
            }
            totalReadSize.add(cache.buffer.limit());
            for (int i = 0; i < versions.size; i++) {
                int ver = versions.vs[i];
                if (ver <= v) {
                    for (int j = 0; j < 64; j++) {
                        fields[j] += cache.buffer.getLong(i * 64 * 8 + j * 8);
                    }
                }
            }
        } else {
            cacheHit.add(1);
            for (int i = 0; i < versions.size; i++) {
                int ver = versions.vs[i];
                if (ver <= v) {
                    for (int j = 0; j < 64; j++) {
                        fields[j] += cache.buffer.getLong(i * 64 * 8 + j * 8);
                    }
                }
            }
        }
        return data;
    }

    public void print() {
        LOG(dir + " dataPosition:" + dataPosition + " keyCounter:" + keyCounter + " index size:" + index.size());
    }

    private void writeKey(long key, long v, int off) throws IOException {
        keyCounter++;
        keyBuf.putLong(key);
        keyBuf.putInt((int) v);
        keyBuf.putInt(off);
        counter.putInt(0, keyCounter);
    }

    public void writeData(ByteBuffer writeBuf, long[] f, long off) throws IOException {
        writeBuf.position(0);
        writeBuf.limit(64 * 8);
        for (long l : f) {
            writeBuf.putLong(l);
        }
        writeBuf.position(0);
        fileChannel.write(writeBuf, off);
    }
}

