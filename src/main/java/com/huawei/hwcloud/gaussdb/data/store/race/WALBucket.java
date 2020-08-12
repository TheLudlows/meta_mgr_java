package com.huawei.hwcloud.gaussdb.data.store.race;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.BUCKET_SIZE;
import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.DEFAULT_SIZE;
import static com.huawei.hwcloud.gaussdb.data.store.race.Counter.*;
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.LOG;
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.LOG_ERR;
import static java.nio.file.StandardOpenOption.*;

public class WALBucket {
    public static final ThreadLocal<Data> LOCAL_DATA = ThreadLocal.withInitial(() -> new Data(64));
    public static final ThreadLocal<ByteBuffer> LOCAL_READ_BUF = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(64 * 8));
    public static final ThreadLocal<ByteBuffer> LOCAL_WRITE_BUF = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(64 * 8));

    protected String dir;
    protected int id;
    // 一个分区总共写入量
    protected int count;
    // 索引
    protected LongObjectHashMap<Versions> index;
    // wal count
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

            tryRecover();
        } catch (Throwable e) {
            LOG_ERR("init bucket error", e);
        }
    }

    private void tryRecover() throws IOException {

        this.count = (int) (keyPosition / 12);
        // 恢复文件数据的索引
        ByteBuffer keyBuf = ByteBuffer.allocate((int) keyPosition);
        // field数据
        ByteBuffer dataBuf = ByteBuffer.allocate((int) dataPosition);
        keyChannel.read(keyBuf, 0);
        fileChannel.read(dataBuf, 0);
        keyBuf.flip();
        dataBuf.flip();
        int off = 0;
        while (keyBuf.hasRemaining()) {
            long k = keyBuf.getLong();
            long v = keyBuf.getInt();
            Versions versions = index.get(k);

            if (versions == null) {
                versions = new Versions(DEFAULT_SIZE);
                index.put(k, versions);
            }
            long[] field = new long[64];
            for (int i = 0; i < 64; i++) {
                field[i] = dataBuf.getLong();
            }
            versions.add(v, off++);
            if(id < BUCKET_SIZE/2) {
                versions.addField(field);
            }
        }
    }

    public synchronized void write(long v, DeltaPacket.DeltaItem item) throws IOException {
        long key = item.getKey();

        ByteBuffer buf = LOCAL_WRITE_BUF.get();
        buf.position(0);
        buf.putLong(key);
        buf.putInt((int) v);
        buf.position(0);
        buf.limit(12);
        keyChannel.write(buf, keyPosition);


        buf.position(0);
        buf.limit(64 * 8);
        for (long l : item.getDelta()) {
            buf.putLong(l);
        }
        buf.position(0);
        fileChannel.write(buf, dataPosition);
        keyPosition += 12;
        dataPosition += 64 * 8;
        Versions versions = index.get(key);
        if (versions == null) {
            versions = new Versions(DEFAULT_SIZE);
            index.put(key, versions);
        }
        versions.add(v, count++);
        if(id < BUCKET_SIZE/2) {
            versions.addField(item.getDelta());
        }
    }

    public Data read(long k, long v) throws IOException {
        Versions versions = index.get(k);
        if (versions == null) {
            return null;
        }
        int size = versions.size;
        Data data = LOCAL_DATA.get();
        data.reset();
        data.setKey(k);
        data.setVersion(v);
        long[] fields = data.getField();

        if (versions.allMatch(v)) {
            allMatchTimes.add(1);
            System.arraycopy(versions.filed, 0, fields, 0, 64);
            data.setField(fields);
            return data;
        }

        for (int i = 0; i < size; i++) {
            long ver = versions.vs[i];
            if (ver <= v) {
                addFiled(versions.off[i], fields);
            }
        }
        return data;
    }

    private void addFiled(Integer n, long[] arr) throws IOException {
        randomRead.add(1);
        totalReadSize.add(64 * 8);
        long pos = n * 64L * 8;
        // 在文件中
        ByteBuffer readBuf = LOCAL_READ_BUF.get();
        readBuf.position(0);
        readBuf.limit(64 * 8);
        fileChannel.read(readBuf, pos);
        for (int i = 0; i < 64; i++) {
            arr[i] += readBuf.getLong(i * 8);
        }

    }

    public void print() {
        LOG(dir + " count:" + count + " index size:" + index.size());
    }
}

