package com.huawei.hwcloud.gaussdb.data.store.race;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.CACHE_SIZE;
import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.page_field_num;

/**
 * @author chender
 * @date 2020/8/16 20:40
 */
public class CacheService {
    private static Map<Long,int[][]>  versionCaches=new ConcurrentHashMap<>(CACHE_SIZE/64/4/page_field_num+1,1);
    private static AtomicInteger cacheLeft =new AtomicInteger(CACHE_SIZE/64/4/page_field_num);
    private static boolean full;
    private static int[][][] initFileds;

    public static void init(){
        initFileds=new int[cacheLeft.get()][][];
        for(int i=0;i<initFileds.length;i++){
            initFileds[i]=new int[4][];
        }
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public static boolean saveCahe(long key,int[] fields,int index){
        int left=-1;
        if(index==0){
            if(full){
                return false;
            }
            left=cacheLeft.decrementAndGet();
            if(left<=0){
                full=true;
                return false;
            }
        }
        //TODO 并发时,先来index2 再来index1的情况
        int[][] exist = versionCaches.get(key);
        if(exist==null){
            if(index!=0){
                return false;
            }
            synchronized (versionCaches) {
                exist = versionCaches.get(key);
                if (exist == null) {
                    exist = initFileds[left];
                    versionCaches.put(key, exist);
                }
            }
        }
        exist[index]=fields;
        return true;

    }

    public static int[][] getCache(long key){
        return versionCaches.get(key);
    }



}
