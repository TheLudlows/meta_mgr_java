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
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.index;
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.mem;
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
        new Thread(() -> {
            try {
                while (true) {
                    LOG(mem());
                    Thread.sleep(MONITOR_TIME);
                }
            } catch (InterruptedException e) {
                LOG_ERR("InterruptedException", e);
            }
        }).start();
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
    private FileChannel keyChannel;
    private ByteBuffer keyWriteBuffer;
    private long keyPosition;
    // data wal， mapped 写入防止丢失
    private MappedByteBuffer wal;
    // 文件中的位置
    private long dataPosition;
    private ByteBuffer readBuffer;
    private FileChannel fileChannel;

    public WALBucket(String dir) {
        try {
            this.dir = dir;
            index = new LongObjectHashMap<>();
            String dataWALName = dir + ".data.wal";
            String dataFileName = dir + ".data";
            String keyFileName = dir + ".key";
            wal = FileChannel.open(new File(dataWALName).toPath(), CREATE, READ, WRITE)
                    .map(FileChannel.MapMode.READ_WRITE, 0, WAL_SIZE);
            keyChannel = FileChannel.open(new File(keyFileName).toPath(), CREATE, READ, WRITE);
            //keyBuffer = FileChannel.open(new File(keyFileName).toPath(), CREATE, READ, WRITE)
            //.map(FileChannel.MapMode.READ_WRITE, 0, KEY_MAPPED_SIZE);
            LOG(dir + " open wal and keyBuffer ok");
            this.fileChannel = FileChannel.open(new File(dataFileName).toPath(), CREATE, READ, WRITE);
            this.readBuffer = ByteBuffer.allocateDirect(64*8);
            this.keyWriteBuffer = ByteBuffer.allocateDirect(16);
            dataPosition = fileChannel.size();
            keyPosition = keyChannel.size();
            LOG(dir + " open data file ok");

            tryRecover();
        } catch (Throwable e) {
            LOG_ERR("init bucket error", e);
        }
    }

    private void tryRecover() throws IOException {
        //count = keyBuffer.getInt(0);
        count = (int) (keyPosition/16);
        int walOff = (int) (count * 64L * 8 - dataPosition);
        wal.position(walOff);
        walCount = walOff / 64 / 8;
        LOG(dir + " walCount:" + walCount + " walOff:" + walOff + " count:" + count);
        if (count > 0) {
            // recover
            ByteBuffer buffer = ByteBuffer.allocateDirect(count * 16);
            keyChannel.read(buffer);
            buffer.flip();

            for (int i = 0; i < count; i++) {
                long k = buffer.getLong();
                long v = buffer.getLong();
                Versions versions = index.get(k);
                if (versions == null) {
                    versions = new Versions(DEFAULT_SIZE);
                    index.put(k, versions);
                }
                versions.add(v, i);
            }
        }

        LOG(dir + " index size:" + index.size());
    }

    public synchronized void write(long v, DeltaPacket.DeltaItem item) throws IOException {
        long key = item.getKey();

        Versions versions = index.get(key);
        if (versions == null) {
            versions = new Versions(DEFAULT_SIZE);
            index.put(key, versions);
        }
        versions.add(v, count++);

        keyWriteBuffer.putLong(0,key);
        keyWriteBuffer.putLong(8,v);
        keyWriteBuffer.position(0);
        keyChannel.write(keyWriteBuffer, keyPosition);
        keyPosition += 16;

        for (long l : item.getDelta()) {
            wal.putLong(l);
        }
        if (++walCount == WAL_COUNT) {
            flush_wal();
            walCount = 0;
            wal.position(0);
        }
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
        wal.limit(WAL_SIZE);
        fileChannel.write(wal, dataPosition);
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
            readBuffer.position(0);
            fileChannel.read(readBuffer, pos);
            for (int i = 0; i < 64; i++) {
                arr[i] += readBuffer.getLong(i * 8);
            }
        }
    }

    public void print() {
        LOG(dir + " count:" + count + " index size:" + index.size());
    }
}

