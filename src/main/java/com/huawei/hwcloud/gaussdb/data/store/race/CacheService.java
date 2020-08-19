package com.huawei.hwcloud.gaussdb.data.store.race;

import com.huawei.hwcloud.gaussdb.data.store.race.utils.BytesUtil;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.*;
import static com.huawei.hwcloud.gaussdb.data.store.race.utils.Util.LOG_ERR;

/**
 * @author chender
 * @date 2020/8/16 20:40
 */
public class CacheService {
    private static ByteBuffer cacheBuffer= ByteBuffer.allocateDirect(cache_capacity+page_size/2);
    private static AtomicInteger cachePosition=new AtomicInteger(-page_size/2);
    private static ConcurrentHashMap<Integer,Versions> positionMap=new ConcurrentHashMap<>(cache_capacity/item_size*2);
    private static volatile boolean full;

    public static void init(){
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public static void saveCahe(long key,int[] fields,byte[] exceed,int index,Versions versions){
        int position=versions.cachePosition;
        if(index==0){//新区
            if(full){
                return;
            }
            position=cachePosition.addAndGet(page_size/2);
            if(position>=cache_capacity-page_size/2||position<0){//已满
                full=true;
                return;
            }else{
                versions.cachePosition=position;
                positionMap.put(position,versions);
            }
//            Versions pre=positionMap.put(position,versions);//覆盖
//            if(pre!=null){
//                pre.cachePosition=-1;
//            }
        }else if(position==-1){
            return;
        }

        if(index>=2){
            if(position%page_size==0){
                Versions pre=positionMap.remove(position+page_size/2);
                if(pre!=null){
                    pre.cachePosition=-1;
                }
            }else{
                return;
            }
        }

        int base=position+index*item_size;
        try {
            for(int i=0;i<64;i++){
                cacheBuffer.putInt(base+i*4,fields[i]);
            }
        }catch (Exception e){
            LOG_ERR("base="+base+",index="+index+",position="+position+",versionSize="+versions.size,e);
            throw e;
        }
        cacheBuffer.putLong(base+field_size,BytesUtil.byteArrToLong(exceed,0));
        cacheBuffer.putLong(base+field_size+8,BytesUtil.byteArrToLong(exceed,8));
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
