package com.huawei.hwcloud.gaussdb.data.store.race;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

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

    private ThreadLocal<FileChannel> LOCALCHANEL;


    private boolean full;

    protected String dir;
    // 索引
    protected LongObjectHashMap<Versions> index;
    protected int id;
    // 文件中的位置
//    protected volatile int dataPosition;
    protected AtomicInteger dataPosition=new AtomicInteger();
    private AtomicInteger keyPosition=new AtomicInteger();
//    private FileChannel fileChannel;
    //    private FileChannel keyChannel;
    private byte[] lock = new byte[0];
    private MappedByteBuffer keyWal;
    private FileChannel[] fileChannels=new FileChannel[30];
    private AtomicInteger fcIndex=new AtomicInteger(-1);

    public WALBucket(String dir, int id) {
        try {
            this.id = id;
            this.dir = dir;
            // 自动扩容吧
            index = new LongObjectHashMap<>(1024 * 8 * 16);
            String dataFileName = dir + ".data";
            String keyWALName = dir + ".key.wal";
//            this.fileChannel = FileChannel.open(new File(dataFileName).toPath(), CREATE, READ, WRITE);

            for(int i=0;i<fileChannels.length;i++){
                fileChannels[i]=FileChannel.open(new File(dataFileName).toPath(), CREATE, READ, WRITE);
            }

            LOCALCHANEL=ThreadLocal.withInitial(()->fileChannels[fcIndex.incrementAndGet()%fileChannels.length]);

//            this.keyChannel = FileChannel.open(new File(keyFileName).toPath(), CREATE, READ, WRITE);
            keyWal = FileChannel.open(new File(keyWALName).toPath(), CREATE, READ, WRITE)
                    .map(FileChannel.MapMode.READ_WRITE, 0, 10 * 1024 * 1024);
            keyWal.position(0);
            keyPosition.set(keyWal.getInt(0));
//            dataPosition = (int)fileChannels[0].size();
            dataPosition.set((int)fileChannels[0].size());

            full=dataPosition.get()>=BUCKET_OPACITY-page_size;

            if (dataPosition.get() % page_size != 0) {
//                dataPosition = (dataPosition.get() / page_size + 1) * page_size;
                LOG("debug,dp:"+dataPosition.get());
                dataPosition.set((dataPosition.get() / page_size + 1) * page_size);
            }
            long start = System.currentTimeMillis();
            tryRecover();
            LOG("Recover " + dir + " cost:" + (System.currentTimeMillis()-start)+",kp:"+keyPosition+",dp:"+dataPosition);
        } catch (Throwable e) {
            LOG_ERR("init bucket error", e);
        }
    }

    private void tryRecover() throws IOException {
        if (keyPosition.get()==0) {
            keyPosition.set(4);
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
//        ByteBuffer keyBuf = ByteBuffer.allocate(keyPosition);
//        ByteBuffer dataBuf = ByteBuffer.allocate(dataPosition);

//        fileChannel.read(dataBuf, 0);
//        keyChannel.read(keyBuf, 0);
        int position=keyPosition.get();
        for (int i = 4; i < position; i += 16) {
            long k = keyWal.getLong(i);
            int v = keyWal.getInt(i + 8);
            int off = keyWal.getInt(i + 12);
            buildIndex(k, v, off,(byte)id);
        }
//        while (keyBuf.hasRemaining()) {
//
//        }
    }

    private void buildIndex(long k, int v, int off,byte bucketIndex) {
        Versions versions = index.get(k);

        if (versions == null) {
            versions = new Versions(DEFAULT_SIZE);
            versions.bucketIndex=bucketIndex;
            index.put(k, versions);
            WALEngine.keyBucketMap.put(k,bucketIndex);
        }
        versions.add(v, off);
//        long[] field = new long[64];
//        for (int i = 0; i < 64; i++) {
//            field[i] = dataBuf.getLong(off + i * 8);
//        }
        //cache
       /* if (id < BUCKET_SIZE / cache_per) {
            versions.addField(field);
        }*/
    }

    public boolean write(long v, DeltaPacket.DeltaItem item) throws IOException {
        long key = item.getKey();
        ByteBuffer writeBuf = LOCAL_WRITE_BUF.get();
        int pos=0;
        Versions versions;
        versions=index.get(key);
        boolean newVersion=false;
        if(versions==null){
            synchronized (lock){
                versions=index.get(key);
                if(versions==null){
                    pos=dataPosition.addAndGet(page_size);
                    if(pos>=BUCKET_OPACITY){
                        return false;
                    }
                    versions = new Versions(DEFAULT_SIZE);
                    versions.bucketIndex=(byte)id;
                    newVersion=true;
                }
            }
        }
        synchronized (versions){
            if (versions.needAlloc()) {
                if(versions.size>4){
                    LOG("debug:unexpecred version size");
                }
//                pos = dataPosition;
//                dataPosition += page_size;
            } else {
                int base = versions.off[versions.size / page_field_num];
                pos = base + (versions.size % page_field_num) * 64 * 8;
            }
            writeData(writeBuf, item.getDelta(), pos);
            versions.add((int) v, pos);
            writeKey(writeBuf, key, v, pos,id);
            if(newVersion){
                index.put(key,versions);
            }
        }

//        synchronized (lock){
//            versions = index.get(key);
//            if (versions == null) {
//                if(dataPosition>=BUCKET_OPACITY){
//                    return false;
//                }
//                versions = new Versions(DEFAULT_SIZE);
//                versions.bucketIndex=(byte)id;
//                index.put(key, versions);
//            }
//            // 计算这个version写盘的位置
//            if (versions.needAlloc()) {
//                if(versions.size>=4){
//                    LOG("unexpecred version size");
//                }
//                pos = dataPosition;
//                dataPosition += page_size;
//            } else {
//                int base = versions.off[versions.size / page_field_num];
//                pos = base + (versions.size % page_field_num) * 64 * 8;
//            }
//            versions.add((int) v, pos);
//            writeKey(writeBuf, key, v, pos,id);
//        }
//        writeData(writeBuf, item.getDelta(), pos);
        return true;
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
                limit=limit > size ? size : limit;
                cache.buffer.limit(limit);
                LOCALCHANEL.get().read(cache.buffer, versions.off[i]);
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

    private void writeKey(ByteBuffer writeBuf, long key, long v, int off,int bucketIndex) throws IOException {
//        writeBuf.position(0);
//        writeBuf.putLong(key);
//        writeBuf.putInt((int) v);
//        writeBuf.putInt(off);
//        writeBuf.limit(16);
//        writeBuf.position(0);
//        keyChannel.write(writeBuf, keyPosition);
            int position=keyPosition.addAndGet(16)-16;
            keyWal.putLong(position, key);
            keyWal.putInt(position + 8, (int) v);
            keyWal.putInt(position + 12, off);
            keyWal.putInt(0, keyPosition.get());
    }

    public void writeData(ByteBuffer writeBuf, long[] f, long off) throws IOException {
        writeBuf.position(0);
        writeBuf.limit(64 * 8);
        for (long l : f) {
            writeBuf.putLong(l);
        }
        writeBuf.position(0);
        LOCALCHANEL.get().write(writeBuf, off);
    }


    public boolean isFull() {
        return full;
    }

    public void setFull(boolean full) {
        this.full = full;
    }
}

