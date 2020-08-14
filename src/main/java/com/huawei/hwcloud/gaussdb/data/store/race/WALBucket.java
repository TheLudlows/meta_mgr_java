package com.huawei.hwcloud.gaussdb.data.store.race;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.*;
import static com.huawei.hwcloud.gaussdb.data.store.race.Counter.cacheHit;
import static com.huawei.hwcloud.gaussdb.data.store.race.Counter.randomRead;
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.LOG;
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.LOG_ERR;
import static java.nio.file.StandardOpenOption.*;

public class WALBucket {
    public static final ThreadLocal<Data> LOCAL_DATA = ThreadLocal.withInitial(() -> new Data(64));
    // 4kb
    public static final ThreadLocal<VersionCache> LOCAL_CACHE = ThreadLocal.withInitial(() -> new VersionCache());

    public static final ThreadLocal<ByteBuffer> LOCAL_WRITE_BUF = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(64 * 8));

    protected String dir;
    // 索引
    protected LongObjectHashMap<Versions> index;
    protected int id;
    // 文件中的位置
    private int dataPosition;
    private int keyPosition;
    private FileChannel fileChannel;
    private FileChannel keyChannel;
    private byte[] lock = new byte[0];

    public WALBucket(String dir, int id) {
        try {
            this.id = id;
            this.dir = dir;
            // 自动扩容吧
            index = new LongObjectHashMap<>(1024 * 8 * 16);
            String dataFileName = dir + ".data";
            String keyFileName = dir + ".key";
            this.fileChannel = FileChannel.open(new File(dataFileName).toPath(), CREATE, READ, WRITE);
            this.keyChannel = FileChannel.open(new File(keyFileName).toPath(), CREATE, READ, WRITE);
            dataPosition = (int) fileChannel.size();
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

        fileChannel.read(dataBuf, 0);
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
        long[] field = new long[64];
        for (int i = 0; i < 64; i++) {
            field[i] = dataBuf.getLong(off + i * 8);
        }
        //cache
       /* if (id < BUCKET_SIZE / cache_per) {
            versions.addField(field);
        }*/
    }

    public void write(long v, DeltaPacket.DeltaItem item) throws IOException {
        long key = item.getKey();
        ByteBuffer writeBuf = LOCAL_WRITE_BUF.get();
        int pos;
        synchronized (lock) {
            Versions versions = index.get(key);
            if (versions == null) {
                versions = new Versions(DEFAULT_SIZE);
                index.put(key, versions);
            }
            // 计算这个version写盘的位置
            if (versions.needAlloc()) {
                pos = dataPosition;
                dataPosition += page_size;
            } else {
                int base = versions.off[versions.size / page_field_num];
                pos = base + (versions.size % page_field_num) * 64 * 8;
            }
            versions.add((int) v, pos);

            writeData(writeBuf, item.getDelta(), pos);
            writeKey(writeBuf, key, v, pos);
        }
        /*if (id < BUCKET_SIZE / cache_per) {
            versions.addField(item.getDelta());
        }*/
    }

    /**
     * for (int i = 0; i < size; i++) {
     * long ver = versions.vs[i];
     * if (ver <= v) {
     * int off = versions.off[i / 8] + (i % 8) * 64 * 8;
     * addFiled(off, fields);
     * //System.out.println(Arrays.toString(fields));
     * }
     * }
     */
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
            int size = versions.size * 64 * 8;
            for (int i = 0; i < versions.off.length; i++) {
                randomRead.add(1);
                int limit = (i + 1) * page_size;
                cache.buffer.limit(limit > size ? size : limit);
                fileChannel.read(cache.buffer, versions.off[i]);
            }
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

