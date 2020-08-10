package com.huawei.hwcloud.gaussdb.data.store.race;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.*;
import static com.huawei.hwcloud.gaussdb.data.store.race.Counter.*;
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.LOG;
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.LOG_ERR;
import static java.nio.file.StandardOpenOption.*;

public class WALBucket {
    public static final ThreadLocal<Data> LOCAL_DATA = ThreadLocal.withInitial(() -> new Data(64));
    // 4kb
    public static final ThreadLocal<ByteBuffer> LOCAL_READ_BUF = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(64 * 8 * 8));
    public static final ThreadLocal<ByteBuffer> LOCAL_WRITE_BUF = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(64 * 8));

    protected String dir;

    // 索引
    protected LongObjectHashMap<Versions> index;
    protected int id;

    // 文件中的位置
    private volatile long dataPosition;
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

            if (dataPosition % page_size != 0) {
                dataPosition = (dataPosition / page_size + 1) * page_size;
            }

            tryRecover();
        } catch (Throwable e) {
            LOG_ERR("init bucket error", e);
        }
    }

    private void tryRecover() throws IOException {
        // 恢复文件数据的索引
        ByteBuffer keyBuf = ByteBuffer.allocate((int) keyPosition);
        ByteBuffer dataBuf = ByteBuffer.allocate((int) dataPosition);

        // field数据
        fileChannel.read(dataBuf, 0);
        keyChannel.read(keyBuf, 0);
        keyBuf.flip();
        while (keyBuf.hasRemaining()) {
            long k = keyBuf.getLong();
            long v = keyBuf.getLong();
            int off = keyBuf.getInt();
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
            if (id < BUCKET_SIZE / 3) {
                versions.addField(field);
            }
        }
    }

    public synchronized void write(long v, DeltaPacket.DeltaItem item) throws IOException {
        long key = item.getKey();
        ByteBuffer writeBuf = LOCAL_WRITE_BUF.get();
        Versions versions = index.get(key);
        if (versions == null) {
            versions = new Versions(DEFAULT_SIZE);
            index.put(key, versions);
        }
        if (versions.needAlloc()) {
            writeKey(writeBuf, key, v, (int) dataPosition, keyPosition);
            writeData(writeBuf, item.getDelta(), dataPosition);
            versions.add(v, (int) dataPosition);
            dataPosition += page_size;
        } else {
            int base = versions.off[versions.size / page_field_num];
            int pos = base + (versions.size % page_field_num) * 64 * 8;
            writeKey(writeBuf, key, v, pos, keyPosition);
            writeData(writeBuf, item.getDelta(), pos);
            versions.add(v, pos);
        }
        if (id < BUCKET_SIZE / 3) {
            versions.addField(item.getDelta());
        }
        keyPosition += 20;
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
        Data data = LOCAL_DATA.get();
        data.reset();
        data.setKey(k);
        data.setVersion(v);
        long[] fields = data.getField();
        // use cache
        if (versions.queryFunc(v) == 0) {
            System.arraycopy(versions.filed, 0, fields, 0, 64);
            return data;
        }
        mergeRead(fields, versions, v);
        return data;
    }

    private boolean mergeRead(long[] fields, Versions versions, long v) throws IOException {
        long[] vs = versions.vs;
        int last = versions.lastLarge(v);

        for (int i = 0; i <= last; ) {
            if (vs[i] > v) {
                i++;
                continue;
            } else {
                addMeetVersion(i, Math.min((i / page_field_num + 1) * page_field_num - 1, last), fields, versions, v);
                i = (i / page_field_num + 1) * page_field_num;
            }
        }
        return true;
    }

    private void addMeetVersion(int first, int last, long[] fields, Versions versions, long v) throws IOException {
        mergeRead.add(1);
        while (versions.vs[last] > v) {
            last--;
        }
        int size = (last - first + 1) * 64 * 8;
        totalReadSize.add(size);
        ByteBuffer readBuf = LOCAL_READ_BUF.get();
        readBuf.position(0);
        readBuf.limit(size);
        long pos = (first % page_field_num) * 64L * 8 + versions.off[first / page_field_num];
        fileChannel.read(readBuf, pos);
        for (int from = first; from <= last; from++) {
            int base = (from - first) * 64 * 8;
            if (versions.vs[from] <= v) {
                for (int j = 0; j < 64; j++) {
                    fields[j] += readBuf.getLong(base + j * 8);
                }
            }
        }
    }


    private void addFiled(int off, long[] arr) throws IOException {
        randomRead.add(1);
        totalReadSize.add(64 * 8);
        // 在文件中
        ByteBuffer readBuf = LOCAL_READ_BUF.get();
        readBuf.position(0);
        readBuf.limit(64 * 8);
        fileChannel.read(readBuf, off);
        for (int i = 0; i < 64; i++) {
            arr[i] += readBuf.getLong(i * 8);
        }
    }

    public void print() {
        LOG(dir + " dataPosition:" + dataPosition + " keyPosition:" + keyPosition + " index size:" + index.size());
    }

    private void writeKey(ByteBuffer writeBuf, long key, long v, int off, long pos) throws IOException {
        writeBuf.position(0);
        writeBuf.putLong(key);
        writeBuf.putLong(v);
        writeBuf.putInt(off);
        writeBuf.limit(20);
        writeBuf.flip();
        keyChannel.write(writeBuf, pos);
    }

    public void writeData(ByteBuffer writeBuf, long[] f, long off) throws IOException {
        writeBuf.position(0);
        writeBuf.limit(64 * 8);
        // field wal
        for (long l : f) {
            writeBuf.putLong(l);
        }
        writeBuf.flip();
        fileChannel.write(writeBuf, off);
    }
}

