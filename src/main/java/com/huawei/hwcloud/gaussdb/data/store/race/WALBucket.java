package com.huawei.hwcloud.gaussdb.data.store.race;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.huawei.hwcloud.gaussdb.data.store.race.utils.BytesUtil;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.*;
//import static com.huawei.hwcloud.gaussdb.data.store.race.Counter.cacheHit;
//import static com.huawei.hwcloud.gaussdb.data.store.race.Counter.randomRead;
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
        private FileChannel keyChannel;
    private byte[] lock = new byte[0];
    private MappedByteBuffer keyWal;

    public WALBucket(String dir, int id) {
        try {
            this.id = id;
            this.dir = dir;
            // 自动扩容吧
            index = new LongObjectHashMap<>();
            String dataFileName = dir + ".data";
            String keyFileName=dir+".key";
            String keyWALName = dir + ".key.wal";
            this.fileChannel = FileChannel.open(new File(dataFileName).toPath(), CREATE, READ, WRITE);
            this.fileChannelRead= FileChannel.open(new File(dataFileName).toPath(),  READ);
            this.keyChannel = FileChannel.open(new File(keyFileName).toPath(), CREATE, READ, WRITE);
            keyWal = FileChannel.open(new File(keyWALName).toPath(), CREATE, READ, WRITE)
                    .map(FileChannel.MapMode.READ_WRITE, 0, key_wal_size);
            keyWal.position(0);
            keyPosition = keyWal.getInt(0);
            dataPosition = (int)fileChannel.size();

            if (dataPosition % page_size != 0) {
                dataPosition = (dataPosition / page_size + 1) * page_size;
            }
            long start = System.currentTimeMillis();
            tryRecover();
            LOG("Recover " + dir + " cost:" + (System.currentTimeMillis()-start)+",kp:"+keyPosition+",dp:"+dataPosition);
        } catch (Throwable e) {
            LOG_ERR("init bucket error", e);
        }
    }

    private void tryRecover() throws IOException {
        if (keyPosition == 0) {
            keyPosition = 4;
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
        int keyLength=(int)keyChannel.size();
        if(keyLength!=0){
            ByteBuffer keyBuf = ByteBuffer.allocate(keyLength);
            keyChannel.read(keyBuf, 0);
            for(int i=0;i<keyLength;i+=16){
                long k = keyBuf.getLong(i);
                int v = keyBuf.getInt(i + 8);
                int off = keyBuf.getInt(i + 12);
                buildIndex(k, v, off,(byte)id);
            }
        }


//        fileChannel.read(dataBuf, 0);
//        keyChannel.read(keyBuf, 0);



        for (int i = 4; i < keyPosition; i += 16) {
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

    public void write(long v, DeltaPacket.DeltaItem item,byte[] exceed) throws IOException {
        long key = item.getKey();
        ByteBuffer writeBuf = LOCAL_WRITE_BUF.get();
        int pos;
        Versions versions;
        synchronized (lock){
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
            writeData(writeBuf, item.getDelta(), exceed,pos);
            versions.add((int) v, pos);
            writeKey(writeBuf, key, v, pos,id);
            if((versions.size==1||versions.cachePosition!=-1)&&versions.size<3){
                versions.cachePosition=CacheService.saveCahe(key,item.getDelta(),exceed,versions.size-1,versions.cachePosition);
            }
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
        data.setKey(k);
        data.setVersion(v);
        long[] fields = data.getField();
        // use cache
        /*if (versions.queryFunc(v) == 0) {
            System.arraycopy(versions.filed, 0, fields, 0, 64);
            return data;
        }*/

        int maxMatchIndex=-1;
        for (int i = 0; i < versions.size; i++) {
            int ver = versions.vs[i];
            if (ver <= v) {
                maxMatchIndex=i;
            }
        }
        if(maxMatchIndex==-1){
            return data;
        }
        int versionSize=versions.size;
        if (cache.key != k||cache.maxMatchIndex<maxMatchIndex) {
            cache.key = k;
            cache.buffer.position(0);
            int skip=0;
            int vcPosition=versions.cachePosition;
            if(vcPosition!=-1){
//                cacheHit.add(1);
                cache.buffer.limit(page_size);
                boolean match;
                if(versionSize<=2){
                    match=CacheService.getCacheData(cache.buffer,vcPosition,versionSize);
                }else{
                    match=CacheService.getCacheData(k,vcPosition,versionSize,cache.buffer);
                }
                if(!match){
                    cache.buffer.position(0);
                    versions.cachePosition=-1;
                    skip=0;
                }else{
                    skip=cache.buffer.position()/item_size;
                }
            }

            int size =versionSize * item_size;
            if(maxMatchIndex+1>skip){
                for (int i = 0; i < versions.off.length; i++) {
//                    randomRead.add(1);
                    int limit = (i + 1) * page_size;
                    limit=limit > size ? size : limit;
                    cache.buffer.limit(limit);
                    if(i==0){
                        fileChannelRead.read(cache.buffer, versions.off[i]+skip*item_size);
                    }else{
                        fileChannelRead.read(cache.buffer, versions.off[i]);
                    }
                }
                if(skip!=0){
                    cache.buffer.position(item_size*skip);
                    CacheService.mergeCache(k,vcPosition,skip,versionSize-skip,cache.buffer);
                }
                cache.maxMatchIndex=versions.size-1;
            }else{
                cache.maxMatchIndex=skip-1;
            }

        }else{
//            cacheHit.add(1);
        }
        for (int i = 0; i < versions.size; i++) {
            int ver = versions.vs[i];
            if (ver <= v) {
                long exceed1=cache.buffer.getLong(i *item_size+field_size);
                long exceed2=cache.buffer.getLong(i *item_size+field_size+8);
                int n;
                for (int j = 0; j < 32; j++) {
                    n=cache.buffer.getInt(i *item_size + j * 4);
                    if(n<0){
                        fields[j] += n+((exceed1>>62-j*2)&0x3)*Integer.MIN_VALUE;
                    }else if(n>0){
                        fields[j] += n+((exceed1>>62-j*2)&0x3)*Integer.MAX_VALUE;
                    }else{
//                        LOG("zore value:"+maxMatchIndex+"/"+versions.size+"/"+i);
                        fields[j] += n+((exceed1>>62-j*2)&0x3)*Integer.MIN_VALUE;
                    }
                }
                for (int j = 32; j < 64; j++) {
                    n=cache.buffer.getInt(i *item_size + j * 4);
                    if(n<0){
                        fields[j] += n + ((exceed2>>62-(j-32)*2)&0x3)*Integer.MIN_VALUE;
                    }else if(n>0){
                        fields[j] += n + ((exceed2>>62-(j-32)*2)&0x3)*Integer.MAX_VALUE;
                    }else{
//                        LOG("zore value1:"+maxMatchIndex+"/"+versions.size+"/"+i);
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
        keyWal.putLong(keyPosition, key);
        keyWal.putInt(keyPosition + 8, (int) v);
        keyWal.putInt(keyPosition + 12, off);
        keyWal.putInt(0, keyPosition + 16);
        keyPosition += 16;
        if(keyPosition>=key_wal_size){
            keyWal.position(4);
            keyWal.limit(key_wal_size);
            keyChannel.write(keyWal);
            keyWal.putLong(0, 4);
            keyPosition=4;
        }
    }

    public void writeData(ByteBuffer writeBuf, int[] f,byte[] exceed, long off) throws IOException {
        writeBuf.position(0);
        writeBuf.limit(item_size);
        for (int l : f) {
            writeBuf.putInt(l);
        }
        writeBuf.putLong(BytesUtil.byteArrToLong(exceed,0));
        writeBuf.putLong(BytesUtil.byteArrToLong(exceed,8));
        writeBuf.position(0);
        fileChannel.write(writeBuf, off);
    }
}

