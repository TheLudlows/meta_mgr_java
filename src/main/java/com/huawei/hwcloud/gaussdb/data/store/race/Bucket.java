package com.huawei.hwcloud.gaussdb.data.store.race;

import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.*;

public class Bucket {
    // todo key v fenli
    private Map<Long, List<Tuple2<Long,Integer>>> index;
    private MappedByteBuffer dataBuffer;
    private MappedByteBuffer counterBuffer;
    private MappedByteBuffer keyBuffer;
    private int count;

    public Bucket(String fileName) {
        try {
            index = new HashMap<>();
            counterBuffer = FileChannel.open(new File(fileName + "counter").toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
                    .map(FileChannel.MapMode.READ_WRITE, 0, 4);
            FileChannel channel = FileChannel.open(new File(fileName + DATA_SUFFIX).toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            dataBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, FILED_MAPPED_SIZE);
            keyBuffer = FileChannel.open(new File(fileName + "key").toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
                    .map(FileChannel.MapMode.READ_WRITE, 0, FILED_MAPPED_SIZE/32);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void  write(long v, DeltaPacket.DeltaItem item) {
        long key = item.getKey();
        List<Tuple2<Long,Integer>> data = index.get(key);
        if(data == null) {
            data = new ArrayList();
            index.put(key,data);
        }
        data.add(new Tuple2<>(v,count));
        keyBuffer.putLong(key);
        keyBuffer.putLong(v);
        for(long l : item.getDelta()) {
            dataBuffer.putLong(l);
        }
        counterBuffer.putInt(0,count++);
    }

    public synchronized Data read(long k, long v) {
        Data data = new Data(k,v);
        List<Tuple2<Long,Integer>> versions = index.get(k);
        if(versions == null) {
            return data;
        }
        long[] fields = new long[64];
        for(Tuple2<Long,Integer> t : versions) {
            if(t.a <= v) {
                long[] delta = getFiled(t.b);
                for (int i = 0; i < delta.length; i++) {
                    fields[i] += delta[i];
                }
            }
        }
        data.setField(fields);
        return data;
    }

    private long[] getFiled(Integer n) {
        byte[] bytes = new byte[64 * 8];
        dataBuffer.get(bytes, n * 64 * 8, 64 * 8);
      long[] filed = new long[64];
        for(int i=0;i<8;i++) {
            filed[i] = (bytes[0 + i*8] << 56)
                    | ((bytes[1+ i*8] & 0xFF) << 48)
                    |  ((bytes[2+ i*8] & 0xFF) << 40)
                    |  ((bytes[3+ i*8] & 0xFF) << 32)
                    |  ((bytes[4+ i*8] & 0xFF) << 24)
                    |  ((bytes[5+ i*8] & 0xFF) << 16)
                    |  ((bytes[6+ i*8] & 0xFF) << 8)
                    |  (bytes[7+ i*8] & 0xFF);
        }
        return filed;
    }
}
