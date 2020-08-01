package com.huawei.hwcloud.gaussdb.data.store.race;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.huawei.hwcloud.gaussdb.data.store.race.utils.Tuple2;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;
import moe.cnkirito.kdio.DirectIOLib;
import moe.cnkirito.kdio.DirectIOUtils;
import moe.cnkirito.kdio.DirectRandomAccessFile;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.*;
import static com.huawei.hwcloud.gaussdb.data.store.race.DataStoreRaceImpl.readCounter;
import static com.huawei.hwcloud.gaussdb.data.store.race.DataStoreRaceImpl.writeCounter;
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.*;
import static java.nio.file.StandardOpenOption.*;

/**
 * y version :mapped file
 * fields :wal -> channel write
 */
public class WALEngine implements DBEngine {
    private String dir;
    private WALBucket buckets[];
    // print bucket
    private Thread backPrint;

    public WALEngine(String dir) {
        this.dir = dir + "/";
        File f = new File(dir);
        if (!f.exists()) {
            f.delete();
            f.mkdir();
        }
        buckets = new WALBucket[BUCKET_SIZE];
    }

    @Override
    public void init() {
        for (int i = 0; i < BUCKET_SIZE; i++) {
            buckets[i] = new WALBucket(dir + i);
        }
        backPrint = new Thread(() ->
        {
            try {
                StringBuffer buffer = new StringBuffer();
                long lastWrite = 0;
                long lastRead = 0;
                while (true) {
                    // buckets
                    buffer.setLength(0);
                    for(WALBucket bucket : buckets) {
                        buffer.append(bucket.dir +" " + bucket.count + " " + bucket.index.size() +"|");
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
    public static final DirectIOLib DIRECT_IO_LIB = DirectIOLib.getLibForPath("/");
    protected String dir;
    // 一个分区总共写入量
    protected int count;
    // wal中的个数
    protected int walCount;
    // 索引
    protected LongObjectHashMap<Tuple2<List<Long>, List<Integer>>> index;
    // key version mapped buffer
    private MappedByteBuffer keyBuffer;
    // data wal， mapped 写入防止丢失
    private MappedByteBuffer wal;
    // 文件中的位置
    private long dataPosition;
    private long keyOff;
    private long walOff;
    private long keyAddress;
    private long walAddress;
    private long writeBufAddress;
    private ByteBuffer writeBuf;
    private boolean DIO_SUPPORT;
    private DirectRandomAccessFile directRandomAccessFile;
    private FileChannel fileChannel;

    public WALBucket(String dir) {
        try {
            //this.DIO_SUPPORT = DirectIOLib.binit;
            this.DIO_SUPPORT = GLOBAL_DIO;
            this.dir = dir;
            index = new LongObjectHashMap<>();
            String dataWALName = dir + ".data.wal";
            String dataFileName = dir + ".data";
            String keyFileName = dir + ".key";
            wal = FileChannel.open(new File(dataWALName).toPath(), CREATE, READ, WRITE)
                    .map(FileChannel.MapMode.READ_WRITE, 0, WAL_SIZE);
            keyBuffer = FileChannel.open(new File(keyFileName).toPath(), CREATE, READ, WRITE)
                    .map(FileChannel.MapMode.READ_WRITE, 0, KEY_MAPPED_SIZE);

            if (DIO_SUPPORT) {
                this.directRandomAccessFile = new DirectRandomAccessFile(new File(dataFileName), "rw");
                this.writeBuf = DirectIOUtils.allocateForDirectIO(DIRECT_IO_LIB, WAL_SIZE);
                dataPosition = directRandomAccessFile.length();
            } else {
                this.fileChannel = FileChannel.open(new File(dataFileName).toPath(), CREATE, READ, WRITE);
                this.writeBuf = ByteBuffer.allocateDirect(WAL_SIZE);
                dataPosition = fileChannel.size();
            }
            tryRecover();
        } catch (IOException e) {
            LOG(e.getMessage());
        }
    }

    private void tryRecover() {
        keyAddress = ((DirectBuffer) keyBuffer).address();
        writeBufAddress = ((DirectBuffer) writeBuf).address();
        walAddress = ((DirectBuffer) wal).address();

        count = UNSAFE.getInt(keyAddress);
        keyOff = 4;
        walOff = count * 64 * 8L - dataPosition;
        walCount = (int) (walOff / 64 / 8);
        LOG("keyAddress:" + keyAddress
                + " writeBufAddress:" + writeBufAddress
                + " walAddress:" + walAddress
                + " walOff:" + walOff
                + " keyOff:" + keyOff
        );
        if (count > 0) {
            // recover
            for (int i = 0; i < count; i++) {
                long k = UNSAFE.getLong(keyAddress + keyOff);
                keyOff += 8;
                long v = UNSAFE.getLong(keyAddress + keyOff);
                keyOff += 8;
                Tuple2<List<Long>, List<Integer>> versions = index.get(k);
                if (versions == null) {
                    versions = new Tuple2<>(new ArrayList<>(), new ArrayList<>());
                    index.put(k, versions);
                }
                versions.a.add(v);
                versions.b.add(i);
            }
        }
        LOG("recover from " + dir + " count:" + count + " index size:" + index.size());

    }

    public synchronized void write(long v, DeltaPacket.DeltaItem item) throws IOException {
        long key = item.getKey();

        Tuple2<List<Long>, List<Integer>> versions = index.get(key);
        if (versions == null) {
            versions = new Tuple2<>(new ArrayList<>(10), new ArrayList<>(10));
            index.put(key, versions);
        }
        versions.a.add(v);
        versions.b.add(count);

        UNSAFE.putLong(keyAddress + keyOff, key);
        keyOff += 8;
        UNSAFE.putLong(keyAddress + keyOff, v);
        keyOff += 8;
        UNSAFE.putInt(keyAddress, ++count);
        for (long l : item.getDelta()) {
            UNSAFE.putLong(walAddress + walOff, l);
            walOff += 8;
        }
        if (++walCount == WAL_COUNT) {
            flush_wal();
            walCount = 0;
            walOff = 0;
        }
    }

    public synchronized Data read(long k, long v) throws IOException {
        Tuple2<List<Long>, List<Integer>> versions = index.get(k);
        if (versions == null) {
            return null;
        }
        Data data = new Data(k, v);
        long[] fields = new long[64];
        for (int i = 0; i < versions.a.size(); i++) {
            long ver = versions.a.get(i);
            if (ver <= v) {
                addFiled(versions.b.get(i), fields);
            }
        }
        data.setField(fields);
        return data;
    }

    private void flush_wal() throws IOException {
        // flush to file
        if (DIO_SUPPORT) {
            UNSAFE.copyMemory(null, walAddress, null, writeBufAddress, WAL_SIZE);
            writeBuf.position(0);
            writeBuf.limit(WAL_SIZE);
            directRandomAccessFile.write(writeBuf, dataPosition);
        } else {
            wal.position(0);
            wal.limit(WAL_SIZE);
            fileChannel.write(wal, dataPosition);
        }
        dataPosition += WAL_SIZE;
    }

    public void flush() {

    }

    private void addFiled(Integer n, long[] arr) throws IOException {
        long pos = n * 64L * 8;
        if (pos >= dataPosition) {
            // 在wal中
            int walPos = (int) (pos - dataPosition);
            for (int i = 0; i < 64; i++) {
                arr[i] += UNSAFE.getLong(walAddress + walPos + i * 8);
            }
        } else {
            // 在文件中
            writeBuf.position(0);
            writeBuf.limit(64 * 8);
            if (DIO_SUPPORT) {
                directRandomAccessFile.read(writeBuf, pos);
            } else {
                fileChannel.read(writeBuf, pos);
            }
            for (int i = 0; i < 64; i++) {
                arr[i] += UNSAFE.getLong(writeBufAddress + i * 8);
            }
        }
    }

    public void print() {
        LOG(dir + " count:" + count + " index size:" + index.size());
    }
}

