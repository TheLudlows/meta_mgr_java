package com.huawei.hwcloud.gaussdb.data.store.race;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.*;
import static com.huawei.hwcloud.gaussdb.data.store.race.Counter.*;
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.LOG;
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.LOG_ERR;
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
    private int keyPosition;
    private FileChannel writeChannel;
    private FileChannel readChannel;

    private FileChannel keyChannel;

    public WALBucket(String dir, int id) {
        try {
            this.id = id;
            this.dir = dir;
            // 自动扩容吧
            index = new LongObjectHashMap<>(1024 * 8 * 16);
            String dataFileName = dir + ".data";
            String keyFileName = dir + ".key";
            this.writeChannel = FileChannel.open(new File(dataFileName).toPath(), CREATE, WRITE);
            this.keyChannel = FileChannel.open(new File(keyFileName).toPath(), CREATE, READ, WRITE);
            dataPosition = (int) writeChannel.size();
            this.readChannel = FileChannel.open(new File(dataFileName).toPath(),READ);

            keyPosition = (int) keyChannel.size();

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
        if (keyPosition == 0) {
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
        ByteBuffer keyBuf = ByteBuffer.allocate(keyPosition);
        ByteBuffer dataBuf = ByteBuffer.allocate(dataPosition);

        readChannel.read(dataBuf, 0);
        keyChannel.read(keyBuf, 0);
        keyBuf.flip();
        while (keyBuf.hasRemaining()) {
            long k = keyBuf.getLong();
            int v = keyBuf.getInt();
            int off = keyBuf.getInt();
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
        int[] field = new int[64];
        for (int i = 0; i < 64; i++) {
            field[i] = dataBuf.getInt(off + i * 4);
        }
        //cache
       /* if (id < BUCKET_SIZE / cache_per) {
            versions.addField(field);
        }*/
    }

    public synchronized void write(long v, DeltaPacket.DeltaItem item) throws IOException {
        writeCounter.add(1);
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
            pos = base + (versions.size % page_field_num) * 64 * 4;
        }
        versions.add((int) v, pos);

        writeData(writeBuf, item.getDelta(), pos);
        writeKey(writeBuf, key, v, pos);
        /*if (id < BUCKET_SIZE / cache_per) {
            versions.addField(item.getDelta());
        }*/
    }

    public Data read(long k, long v) throws IOException {
        Versions versions = index.get(k);
        if (versions == null) {
            return null;
        }
        VersionCache cache = LOCAL_CACHE.get();
        Data data = cache.data;
        data.reset();
        data.setVersion(v);
        long[] fields = data.getField();
        // use cache
        /*if (versions.queryFunc(v) == 0) {
            System.arraycopy(versions.filed, 0, fields, 0, 64);
            return data;
        }*/

        if (cache.key != k) {
            data.setKey(k);
            cache.key = k;
            cache.buffer.position(0);
            int size = versions.size * 64 * 4;
            for (int i = 0; i < versions.off.length; i++) {
                randomRead.add(1);
                int limit = (i + 1) * page_size;
                cache.buffer.limit(limit > size ? size : limit);
                readChannel.read(cache.buffer, versions.off[i]);
            }
            totalReadSize.add(cache.buffer.limit());
            for (int i = 0; i < versions.size; i++) {
                int ver = versions.vs[i];
                if (ver <= v) {
                    for (int j = 0; j < 64; j++) {
                        fields[j] += cache.buffer.getInt(i * 64 * 4 + j * 4);
                    }
                }
            }
        } else {
            cacheHit.add(1);
            for (int i = 0; i < versions.size; i++) {
                int ver = versions.vs[i];
                if (ver <= v) {
                    for (int j = 0; j < 64; j++) {
                        fields[j] += cache.buffer.getInt(i * 64 * 4 + j * 4);
                    }
                }
            }
        }
        return data;
    }

    public void print() {
        LOG(dir + " dataPosition:" + dataPosition + " keyPosition:" + keyPosition + " index size:" + index.size());
    }

    private void writeKey(ByteBuffer writeBuf, long key, long v, int off) throws IOException {
        writeBuf.position(0);
        writeBuf.putLong(key);
        writeBuf.putInt((int) v);
        writeBuf.putInt(off);
        writeBuf.limit(16);
        writeBuf.position(0);
        keyChannel.write(writeBuf, keyPosition);
        keyPosition += 16;
    }

    public void writeData(ByteBuffer writeBuf, int[] f, long off) throws IOException {
        writeBuf.position(0);
        writeBuf.limit(64 * 4);
        for (int l : f) {
            writeBuf.putInt(l);
        }
        writeBuf.position(0);
        writeChannel.write(writeBuf, off);
    }
}

