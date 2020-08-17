package com.huawei.hwcloud.gaussdb.data.store.race;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.huawei.hwcloud.gaussdb.data.store.race.utils.BytesUtil;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.*;
import static com.huawei.hwcloud.gaussdb.data.store.race.Counter.randomRead;
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.LOG;
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.LOG_ERR;
import static java.nio.file.StandardOpenOption.*;

public class WALBucket {
    public static final ThreadLocal<VersionCache> LOCAL_CACHE = ThreadLocal.withInitial(() -> new VersionCache());

    public static final ThreadLocal<ByteBuffer> LOCAL_WRITE_BUF = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(item_size));

    protected String dir;
    // 索引
    protected LongObjectHashMap<Versions> index;
    protected int id;
    // 文件中的位置
    private int dataPosition;
    private int keyPosition;
    private FileChannel fileChannel;
    private FileChannel fileChannelRead;
    //    private FileChannel keyChannel;
    private byte[] lock = new byte[0];
    private MappedByteBuffer keyWal;

    public WALBucket(String dir, int id) {
        try {
            this.id = id;
            this.dir = dir;
            // 自动扩容吧
            index = new LongObjectHashMap<>(1024 * 8 * 16);
            String dataFileName = dir + ".data";
            String keyWALName = dir + ".key.wal";
            this.fileChannel = FileChannel.open(new File(dataFileName).toPath(), CREATE, READ, WRITE);
            this.fileChannelRead = FileChannel.open(new File(dataFileName).toPath(), READ);
            keyWal = FileChannel.open(new File(keyWALName).toPath(), CREATE, READ, WRITE)
                    .map(FileChannel.MapMode.READ_WRITE, 0, 34 * 1024 * 1024);
            keyWal.position(0);
            keyPosition = keyWal.getInt(0);
            dataPosition = (int) fileChannel.size();

            if (dataPosition % page_size != 0) {
                dataPosition = (dataPosition / page_size + 1) * page_size;
            }
            long start = System.currentTimeMillis();
            tryRecover();
            LOG("Recover " + dir + " cost:" + (System.currentTimeMillis() - start) + ",kp:" + keyPosition + ",dp:" + dataPosition);
        } catch (Throwable e) {
            LOG_ERR("init bucket error", e);
        }
    }

    private void tryRecover() {
        if (keyPosition == 0) {
            keyPosition = 4;
            return;
        }
        for (int i = 4; i < keyPosition; i += 16) {
            long k = keyWal.getLong(i);
            int v = keyWal.getInt(i + 8);
            int off = keyWal.getInt(i + 12);
            buildIndex(k, v, off, (byte) id);
        }
    }

    private void buildIndex(long k, int v, int off, byte bucketIndex) {
        Versions versions = index.get(k);

        if (versions == null) {
            versions = new Versions(DEFAULT_SIZE);
            index.put(k, versions);
            WALEngine.keyBucketMap.put(k, bucketIndex);
        }
        versions.add(v, off);
    }

    public void write(long v, DeltaPacket.DeltaItem item) throws IOException {
        long key = item.getKey();
        ByteBuffer writeBuf = LOCAL_WRITE_BUF.get();
        int pos;
        Versions versions;
        synchronized (lock) {
            versions = index.get(key);
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
                pos = base + (versions.size % page_field_num) * item_size;
            }
            writeData(writeBuf, item.getDelta(), item.getExceed(), pos);
            versions.add((int) v, pos);
            writeKey(key, v, pos);
            CacheService.saveCahe(key, item.getDelta(), item.getExceed(), versions.size - 1);
        }
    }

    public Data read(long k, long v) throws IOException {
        Versions versions = index.get(k);
        if (versions == null) {
            return null;
        }
        VersionCache cache = LOCAL_CACHE.get();
        Data data = cache.data;
        data.reset();
        data.setKey(k);
        data.setVersion(v);
        long[] fields = data.getField();
        // use cache
        int[][] caches = CacheService.getCacheData(k);
        if (caches != null) {
            byte[][] exceeds = CacheService.getCacheExceed(k);
            for (int i = 0; i < versions.size; i++) {
                int ver = versions.vs[i];
                if (ver <= v) {
                    long exceed1 = BytesUtil.byteArrToLong(exceeds[i], 0);
                    long exceed2 = BytesUtil.byteArrToLong(exceeds[i], 8);
                    int n;
                    for (int j = 0; j < 32; j++) {
                        n = caches[i][j];
                        if (n < 0) {
                            fields[j] += n + ((exceed1 >> 62 - j * 2) & 0x3) * Integer.MIN_VALUE;
                        } else if (n > 0) {
                            fields[j] += n + ((exceed1 >> 62 - j * 2) & 0x3) * Integer.MAX_VALUE;
                        } else {
                            LOG("zore value");
                            fields[j] += n + ((exceed1 >> 62 - j * 2) & 0x3) * Integer.MIN_VALUE;
                        }
                    }
                    for (int j = 32; j < 64; j++) {
                        n = caches[i][j];
                        if (n < 0) {
                            fields[j] += n + ((exceed2 >> 62 - (j - 32) * 2) & 0x3) * Integer.MIN_VALUE;
                        } else if (n > 0) {
                            fields[j] += n + ((exceed2 >> 62 - (j - 32) * 2) & 0x3) * Integer.MAX_VALUE;
                        } else {
                            LOG("zore value");
                        }
                    }
                }
            }
            return data;
        }

        if (cache.key != k) {
            cache.key = k;
            cache.buffer.position(0);
            int size = versions.size * item_size;
            for (int i = 0; i < versions.off.length; i++) {
                randomRead.add(1);
                int limit = (i + 1) * page_size;
                limit = limit > size ? size : limit;
                cache.buffer.limit(limit);
                fileChannelRead.read(cache.buffer, versions.off[i]);
            }
        }
        for (int i = 0; i < versions.size; i++) {
            int ver = versions.vs[i];
            if (ver <= v) {
                long exceed1 = cache.buffer.getLong(i * item_size + field_size);
                long exceed2 = cache.buffer.getLong(i * item_size + field_size + 8);
                int n;
                for (int j = 0; j < 32; j++) {
                    n = cache.buffer.getInt(i * item_size + j * 4);
                    if (n < 0) {
                        fields[j] += n + ((exceed1 >> 62 - j * 2) & 0x3) * Integer.MIN_VALUE;
                    } else if (n > 0) {
                        fields[j] += n + ((exceed1 >> 62 - j * 2) & 0x3) * Integer.MAX_VALUE;
                    } else {
                        LOG("zore value");
                        fields[j] += n + ((exceed1 >> 62 - j * 2) & 0x3) * Integer.MIN_VALUE;
                    }
                }
                for (int j = 32; j < 64; j++) {
                    n = cache.buffer.getInt(i * item_size + j * 4);
                    if (n < 0) {
                        fields[j] += n + ((exceed2 >> 62 - (j - 32) * 2) & 0x3) * Integer.MIN_VALUE;
                    } else if (n > 0) {
                        fields[j] += n + ((exceed2 >> 62 - (j - 32) * 2) & 0x3) * Integer.MAX_VALUE;
                    } else {
                        LOG("zore value");
                    }
                }
            }
        }
        return data;
    }

    public void print() {
        LOG(dir + " dataPosition:" + dataPosition + " keyPosition:" + keyPosition + " index size:" + index.size());
    }

    private void writeKey(long key, long v, int off) {
        keyWal.putLong(keyPosition, key);
        keyWal.putInt(keyPosition + 8, (int) v);
        keyWal.putInt(keyPosition + 12, off);
        keyWal.putInt(0, keyPosition + 16);
        keyPosition += 16;
    }

    public void writeData(ByteBuffer writeBuf, int[] f, byte[] exceed, long off) throws IOException {
        writeBuf.position(0);
        writeBuf.limit(item_size);
        for (int l : f) {
            writeBuf.putInt(l);
        }
        writeBuf.putLong(BytesUtil.byteArrToLong(exceed, 0));
        writeBuf.putLong(BytesUtil.byteArrToLong(exceed, 8));
        writeBuf.position(0);
        fileChannel.write(writeBuf, off);
    }
}

