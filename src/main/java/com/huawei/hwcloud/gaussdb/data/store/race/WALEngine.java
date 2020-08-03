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
                while (true) {
                    // buckets
                    buffer.setLength(0);
                    for (WALBucket bucket : buckets) {
                        buffer.append(bucket.dir + " " + bucket.count + " " + bucket.index.size() + "|");
                    }
                    LOG(buffer.toString());
                    // request
                    long read = readCounter.sum();
                    long write = writeCounter.sum();
                    LOG("last read:" + (read - lastRead) + " last write:" + (write - lastWrite));
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
}

class WALBucket {
    public static final ThreadLocal<Data> LOCAL_DATA = ThreadLocal.withInitial(() -> new Data());
    protected String dir;
    // 一个分区总共写入量
    protected int count;
    // wal中的个数
    protected int walCount;
    // 索引
    protected LongObjectHashMap<Versions> index;
    // key version mapped buffer
    private MappedByteBuffer keyWal;
    // data wal， mapped 写入防止丢失
    private MappedByteBuffer wal;
    // wal count
    private MappedByteBuffer counterBuf;
    // 文件中的位置
    private long dataPosition;
    private long keyPosition;
    private ByteBuffer writeBuf;
    private FileChannel fileChannel;
    private FileChannel keyChannel;

    public WALBucket(String dir) {
        try {
            this.dir = dir;
            index = new LongObjectHashMap<>();
            String dataWALName = dir + ".data.wal";
            String keyWALName = dir + ".key.wal";
            String counter = dir + ".count";
            String dataFileName = dir + ".data";
            String keyFileName = dir + ".key";
            wal = FileChannel.open(new File(dataWALName).toPath(), CREATE, READ, WRITE)
                    .map(FileChannel.MapMode.READ_WRITE, 0, WAL_SIZE);
            keyWal = FileChannel.open(new File(keyWALName).toPath(), CREATE, READ, WRITE)
                    .map(FileChannel.MapMode.READ_WRITE, 0, KEY_MAPPED_SIZE);
            counterBuf = FileChannel.open(new File(counter).toPath(), CREATE, READ, WRITE)
                    .map(FileChannel.MapMode.READ_WRITE, 0, 4);

            this.fileChannel = FileChannel.open(new File(dataFileName).toPath(), CREATE, READ, WRITE);
            this.keyChannel = FileChannel.open(new File(keyFileName).toPath(), CREATE, READ, WRITE);
            this.writeBuf = ByteBuffer.allocateDirect(WAL_SIZE);
            dataPosition = fileChannel.size();
            keyPosition = keyChannel.size();

            tryRecover();
        } catch (Throwable e) {
            LOG_ERR("init bucket error", e);
        }
    }

    private void tryRecover() throws IOException {
        walCount = counterBuf.getInt(0);
        wal.position(walCount * 64 * 8);
        keyWal.position(walCount * 16);
        int fileCount = (int) (keyPosition / 16);
        this.count = walCount + fileCount;
        // 恢复文件数据的索引
        ByteBuffer buf = ByteBuffer.allocate(fileCount * 16);
        keyChannel.read(buf, 0);
        buf.flip();
        int off = 0;
        while (buf.hasRemaining()) {
            long k = buf.getLong();
            long v = buf.getLong();
            Versions versions = index.get(k);

            if (versions == null) {
                versions = new Versions(DEFAULT_SIZE);
                index.put(k, versions);
            }
            versions.add(v, off++);
        }
        // wal中的索引
        int walOff = 0;
        if (walCount > 0) {
            // recover
            for (int i = 0; i < walCount; i++) {
                long k = keyWal.getLong(walOff);
                walOff += 8;
                long v = keyWal.getLong(walOff);
                walOff += 8;
                Versions versions = index.get(k);
                if (versions == null) {
                    versions = new Versions(DEFAULT_SIZE);
                    index.put(k, versions);
                }
                versions.add(v, off++);
            }
        }

        LOG(dir + " index size:" + index.size() + " walCount:" + walCount);
    }

    public synchronized void write(long v, DeltaPacket.DeltaItem item) throws IOException {
        long key = item.getKey();

        Versions versions = index.get(key);
        if (versions == null) {
            versions = new Versions(DEFAULT_SIZE);
            index.put(key, versions);
        }
        versions.add(v, count++);

        keyWal.putLong(key);
        keyWal.putLong(v);
        for (long l : item.getDelta()) {
            wal.putLong(l);
        }
        if (++walCount == WAL_COUNT) {
            flush_wal();
            walCount = 0;
            wal.position(0);
            keyWal.position(0);
        }
        counterBuf.putInt(0, walCount);
    }

    public synchronized Data read(long k, long v) throws IOException {
        Versions versions = index.get(k);
        if (versions == null) {
            return null;
        }
        Data data = LOCAL_DATA.get();
        data.reset();
        data.setKey(k);
        data.setVersion(v);
        long[] fields = data.getField();
        boolean find = false;
        for (int i = 0; i < versions.size; i++) {
            long ver = versions.vs[i];
            if (ver <= v) {
                find = true;
                addFiled(versions.off[i], fields);
            }
        }
        return find ? data : null;
    }

    private void flush_wal() throws IOException {
        // flush to file
        wal.position(0);
        fileChannel.write(wal, dataPosition);

        keyWal.position(0);
        keyChannel.write(keyWal, keyPosition);

        keyPosition += KEY_MAPPED_SIZE;
        dataPosition += WAL_SIZE;
    }

    private void addFiled(Integer n, long[] arr) throws IOException {
        long pos = n * 64L * 8;
        if (pos >= dataPosition) {
            // 在wal中
            int walPos = (int) (pos - dataPosition);
            for (int i = 0; i < 64; i++) {
                arr[i] += wal.getLong(walPos + i * 8);
            }
        } else {
            // 在文件中
            writeBuf.position(0);
            writeBuf.limit(64 * 8);
            fileChannel.read(writeBuf, pos);
            for (int i = 0; i < 64; i++) {
                arr[i] += writeBuf.getLong(i * 8);
            }
        }
    }

    public void print() {
        LOG(dir + " count:" + count + " index size:" + index.size());
    }
}

