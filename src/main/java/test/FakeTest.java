package test;

import com.huawei.hwcloud.gaussdb.data.store.race.DataStoreRace;
import com.huawei.hwcloud.gaussdb.data.store.race.DataStoreRaceImpl;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 模拟本地测试
 */
public class FakeTest {
    static int thread_n = 30;
    static int n = 10000;
    private static ExecutorService executorService= Executors.newFixedThreadPool(30);
    private static List<CompletableFuture<Object>> futures;

    public static void main(String[] args) throws InterruptedException {
        DataStoreRace store = new DataStoreRaceImpl();
        store.init("data");
        long start = System.currentTimeMillis();

        List<Set<DeltaPacket>> datas = buildPacket(thread_n,0,1,0,thread_n*n);
        List<Set<DeltaPacket>> datas2 = buildPacket(thread_n/2,2,3,0,thread_n*n);

        System.out.println("prepare data cost:"+(System.currentTimeMillis()-start));
        Thread.sleep(2000);

        start=System.currentTimeMillis();
        futures=new ArrayList<>(thread_n);
        for (int i = 0; i < thread_n; i++) {//写
            Set<DeltaPacket> set=datas.get(i);
            futures.add(CompletableFuture.supplyAsync(()->{
                write(store,set);
                return null;
            },executorService));
        }
        start();
        System.out.println("write over cost:" + ((System.currentTimeMillis() - start) ));


        futures=new ArrayList<>(thread_n);
        start = System.currentTimeMillis();
        for (int i = 0; i < thread_n; i++) {//读
            int x = i;
            futures.add(CompletableFuture.supplyAsync(()->{
                read(store, x * n, (x + 1) * n,0,1);
                return null;
            },executorService));
        }
        start();
        System.out.println("read cost: " + (System.currentTimeMillis() - start) );


        Thread.sleep(2000);

        futures=new ArrayList<>(thread_n);
        start=System.currentTimeMillis();//读写混合阶段

        for (int i = 0; i < thread_n/2; i++) {//读之前的
            int x = i;
            futures.add(CompletableFuture.supplyAsync(()->{
                read(store, x * n, (x + 1) * n,0,1);
                return null;
            },executorService));
        }

        for (int i = 0; i < thread_n/2; i++) {//写新的
            Set<DeltaPacket> set=datas2.get(i);
            futures.add(CompletableFuture.supplyAsync(()->{
                write(store,set);
                return null;
            },executorService));
        }
        start();

        futures=new ArrayList<>(thread_n/2);
        for (int i =thread_n/2; i < thread_n; i++) {//读新的
            int x = i;
            futures.add(CompletableFuture.supplyAsync(()->{
                read(store, x * n, (x + 1) * n,0,3);
                return null;
            },executorService));
        }
        start();
        System.out.println("mix cost: " + (System.currentTimeMillis() - start) );

        executorService.shutdown();
        store.deInit();
    }

    private static void read(DataStoreRace store, int ks, int ke,int vs,int ve) {
        for (int i = ks; i < ke; i++) {
            for(int j=vs;j<=ve;j++){
                Data data = store.readDataByVersion(Integer.MIN_VALUE+i, /*ThreadLocalRandom.current().nextInt(9999)*/j);
                if (data != null) {
                    if (data.getField()[0] != (Integer.MIN_VALUE+i) *2L* (j+1)) {
                        System.out.println(data);
                        System.out.println();
                        System.exit(1);
                    }
                }
            }
        }
    }

    public static int[] randomDelta(int d) {
        int[] longs = new int[64];
        for (int i = 0; i < 64; i++) {
            longs[i] = d;
        }
        return longs;
    }

    public static void write(DataStoreRace dataStoreRace, Set<DeltaPacket> deltaPackets) {
        for(DeltaPacket deltaPacket:deltaPackets){
            dataStoreRace.writeDeltaPacket(deltaPacket);
        }
    }

    private static List<Set<DeltaPacket>> buildPacket(int batchSize,int versionStart, int versionEnd, int ks,int ke){
        List<Set<DeltaPacket>> result=new ArrayList<>();
        for(int i=0;i<batchSize;i++){
            result.add(new HashSet<>());
        }
        for (int i = ks; i < ke; i++) {
            for (int j = versionStart; j <= versionEnd; j++) {
                DeltaPacket deltaPacket = new DeltaPacket();
                deltaPacket.setDeltaItem(new ArrayList<>(1));
                deltaPacket.setDeltaCount((short)2);
                deltaPacket.setVersion(j);
                DeltaPacket.DeltaItem item = new DeltaPacket.DeltaItem();
                item.setKey(Integer.MIN_VALUE+i);
                item.setDelta(randomDelta(Integer.MIN_VALUE+i));
                deltaPacket.getDeltaItem().add(item);

                item = new DeltaPacket.DeltaItem();
                item.setKey(Integer.MIN_VALUE+i);
                item.setDelta(randomDelta(Integer.MIN_VALUE+i));
                deltaPacket.getDeltaItem().add(item);

                result.get((int)(Math.random()*batchSize)).add(deltaPacket);
            }
        }
        return result;
    }

    public static void start() throws InterruptedException {
       for(CompletableFuture<Object> future:futures){
           try {
               future.get();
           } catch (ExecutionException e) {
               e.printStackTrace();
           }
       }
    }
}
