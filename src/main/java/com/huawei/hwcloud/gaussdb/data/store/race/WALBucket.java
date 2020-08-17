package com.huawei.hwcloud.gaussdb.data.store.race;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.DEFAULT_SIZE;
import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.field_size;
import static com.huawei.hwcloud.gaussdb.data.store.race.Counter.randomRead;
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.LOG;
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.LOG_ERR;
import static java.nio.file.StandardOpenOption.*;

public class WALBucket {
    public static final ThreadLocal<VersionCache> LOCAL_DATA = ThreadLocal.withInitial(() -> new VersionCache());
    public static final ThreadLocal<ByteBuffer> LOCAL_WRITE_BUF = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(field_size * 16));

    protected String dir;
    protected int id;
    // 一个分区总共写入量
    protected int count;
    // 索引
    protected LongObjectHashMap<Versions> index;
    // 文件中的位置
    private long dataPosition;
    private long keyPosition;
    private FileChannel fileChannel;
    private FileChannel keyChannel;

    public WALBucket(String dir, int id) {
        try {
            this.id = id;
            this.dir = dir;
            // 自动扩容吧
            index = new LongObjectHashMap<>();
            String dataFileName = dir + ".data";
            String keyFileName = dir + ".key";
            this.fileChannel = FileChannel.open(new File(dataFileName).toPath(), CREATE, READ, WRITE);
            this.keyChannel = FileChannel.open(new File(keyFileName).toPath(), CREATE, READ, WRITE);
            dataPosition = fileChannel.size();
            keyPosition = keyChannel.size();
            tryRecover();
        } catch (Throwable e) {
            LOG_ERR("init bucket error", e);
        }
    }

    private void tryRecover() throws IOException {

        this.count = (int) (keyPosition / 12);
        // 恢复文件数据的索引
        ByteBuffer keyBuf = ByteBuffer.allocate((int) keyPosition);
        keyChannel.read(keyBuf, 0);
        keyBuf.flip();
        int off = 0;
        while (keyBuf.hasRemaining()) {
            long k = keyBuf.getLong();
            long v = keyBuf.getInt();
            Versions versions = index.get(k);

            if (versions == null) {
                versions = new Versions(DEFAULT_SIZE);
                index.put(k, versions);
            }
            versions.add((int) v, off++);
        }
    }

    public synchronized void write(long v, DeltaPacket.DeltaItem item) throws IOException {
        long key = item.getKey();
        long[] field = item.getDelta();
        Versions versions = index.get(key);
        if (versions == null) {
            versions = new Versions(DEFAULT_SIZE);
            index.put(key, versions);
            versions.add((int) v, count++);
            appendData(key, v, field);
        } else {
            ByteBuffer buf = LOCAL_WRITE_BUF.get();
            buf.position(0);
            buf.limit(versions.size * field_size);
            fileChannel.read(buf, versions.off[0] * 64 * 8);
            buf.limit(versions.size * field_size + field_size);
            for (int i = 0; i < 64; i++) {
                buf.putLong(field[i]);
            }
            buf.flip();
            fileChannel.write(buf, dataPosition);
            // 更新索引
            for (int i = 0; i < versions.size; i++) {
                versions.off[i] = count++;
            }
            versions.add((int) v, count++);
            dataPosition += buf.limit();
            // 更新key
            buf.position(0);
            buf.limit(versions.size * 12);
            for (int i = 0; i < versions.size; i++) {
                buf.putLong(key);
                buf.putInt(versions.vs[i]);
            }
            buf.flip();
            keyChannel.write(buf, keyPosition);
            keyPosition += buf.limit();
        }

    }

    public void appendData(long key, long v, long[] field) throws IOException {
        ByteBuffer buf = LOCAL_WRITE_BUF.get();
        buf.position(0);
        buf.putLong(key);
        buf.putInt((int) v);
        buf.position(0);
        buf.limit(12);
        keyChannel.write(buf, keyPosition);
        buf.position(0);
        buf.limit(field_size);
        for (long l : field) {
            buf.putLong(l);
        }
        buf.position(0);
        fileChannel.write(buf, dataPosition);
        keyPosition += 12;
        dataPosition += field_size;
    }

    public Data read(long k, long v) throws IOException {
        Versions versions = index.get(k);
        if (versions == null) {
            return null;
        }
        int size = versions.size;
        VersionCache cache = LOCAL_DATA.get();
        Data data = cache.data;
        ByteBuffer buf = cache.buffer;
        data.reset();
        data.setKey(k);
        data.setVersion(v);
        long[] fields = data.getField();
        if (cache.key != k) {
            data.setKey(k);
            cache.key = k;
            buf.position(0);
            for (int i = 0; i < size; i++) {
                randomRead.add(1);
                buf.limit((i + 1) * 64 * 8);
                fileChannel.read(buf, versions.off[i] * 64 * 8);
            }
            for (int i = 0; i < size; i++) {
                int ver = versions.vs[i];
                if (ver <= v) {
                    for (int j = 0; j < 64; j++) {
                        fields[j] += buf.getLong(i * 64 * 8 + j * 8);
                    }
                }
            }
        } else {
            //cacheHit.add(1);
            for (int i = 0; i < size; i++) {
                int ver = versions.vs[i];
                if (ver <= v) {
                    for (int j = 0; j < 64; j++) {
                        fields[j] += buf.getLong(i * 64 * 8 + j * 8);
                    }
                }
            }
        }
        return data;
    }

    public void print() {
        LOG(dir + " count:" + count + " index size:" + index.size());
    }
}

