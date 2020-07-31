package com.huawei.hwcloud.gaussdb.data.store.race;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.*;
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.UNSAFE;
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.index;
import static java.nio.file.StandardOpenOption.*;

public class DioEngine implements DBEngine {
    private String dir;
    private DioBucket buckets[];

    public DioEngine(String dir) {
        this.dir = dir + "/";
        buckets = new DioBucket[BUCKET_SIZE];
    }

    @Override
    public void init() {
        for (int i = 0; i < BUCKET_SIZE; i++) {
            buckets[i] = new DioBucket(dir + i);
        }
    }
    @Override
    public void write(long v, DeltaPacket.DeltaItem item) throws IOException {
        buckets[index(item.getKey())].write(v, item);
    }

    @Override
    public void print() {
        for (DioBucket b : buckets) {
            b.print();
        }
    }

    @Override
    public Data read(long key, long v) throws IOException {
        return buckets[index(key)].read(key, v);
    }
}

class DioBucket {
    public static final DirectIOLib DIRECT_IO_LIB = DirectIOLib.getLibForPath("/");
    private String dir;
    // 一个分区总共写入量
    private int count;
    // wal中的个数
    private int walCount;
    // 索引
    private Map<Long, List<Tuple2<Long, Integer>>> index;
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

    public DioBucket(String dir) {
        try {
            //this.DIO_SUPPORT = DirectIOLib.binit;
            this.DIO_SUPPORT = GLOBAL_DIO;
            this.dir = dir;
            index = new HashMap<>(1024*16*4);
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
        walOff = count * 64 * 8 - dataPosition;
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
                List<Tuple2<Long, Integer>> versions = index.get(k);
                if (versions == null) {
                    versions = new ArrayList<>();
                    index.put(k, versions);
                }
                versions.add(new Tuple2<>(v, i));
            }
        }
        LOG("recover from " + dir + " count:" + count + " index size:" + index.size());

    }

    public synchronized void write(long v, DeltaPacket.DeltaItem item) throws IOException {
        long key = item.getKey();
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
            flush_page();
            walCount = 0;
            walOff = 0;
        }
        List<Tuple2<Long, Integer>> data = index.get(key);
        if (data == null) {
            data = new ArrayList();
            index.put(key, data);
        }
        data.add(new Tuple2<>(v, count));
    }

    public synchronized Data read(long k, long v) throws IOException {
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

    private void flush_page() throws IOException {
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

    private long[] getFiled(Integer n) throws IOException {
        long[] filed = new long[64];
        long pos = n * 64L * 8;
        if (pos > dataPosition) {
            // 在wal中
            int walPos = (int) (pos - dataPosition);
            for (int i = 0; i < 64; i++) {
                filed[i] = UNSAFE.getLong(walAddress + walPos + i * 8);
            }
        } else {
            // 在文件中
            writeBuf.clear();
            writeBuf.limit(64 * 8);
            if (DIO_SUPPORT) {
                directRandomAccessFile.read(writeBuf, pos);
            } else {
                fileChannel.read(writeBuf, pos);
            }
            for (int i = 0; i < 64; i++) {
                filed[i] = UNSAFE.getLong(writeBufAddress + i * 8);
            }
        }
        return filed;
    }

    public void print() {
        LOG(dir + " count:" + count);
    }
}

