package com.huawei.hwcloud.gaussdb.data.store.race;

import com.huawei.hwcloud.gaussdb.data.store.race.utils.BytesUtil;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.*;

/**
 * @author chender
 * @date 2020/8/16 20:40
 */
public class CacheService {
    private static ByteBuffer cacheBuffer= ByteBuffer.allocateDirect(cache_capacity);
    private static AtomicInteger cachePosition=new AtomicInteger(-item_size);
    private static volatile boolean full;

    public static void init(){
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public static int saveCahe(long key,int[] fields,byte[] exceed,int index,int position){
        if(position==-1&&(full||(position=cachePosition.addAndGet(page_size/2))>=cache_capacity-page_size)){
            full=true;
            return -1;
        }
        int base=index*item_size+position;
        for(int i=0;i<64;i++){
            cacheBuffer.putInt(base+i*4,fields[i]);
        }
        cacheBuffer.putLong(base+field_size,BytesUtil.byteArrToLong(exceed,0));
        cacheBuffer.putLong(base+field_size+8,BytesUtil.byteArrToLong(exceed,8));
        return position;

    }

    public static void getCacheData(long key,ByteBuffer byteBuffer,int position,int versionSize){
        for(int j=0;j<versionSize;j++){
            int base=j*item_size+position;
            for(int i=0;i<64;i++){
                byteBuffer.putInt(cacheBuffer.getInt(base+i*4));
            }
            byteBuffer.putLong(cacheBuffer.getLong(base+field_size));
            byteBuffer.putLong(cacheBuffer.getLong(base+field_size+8));
        }
    }

}
