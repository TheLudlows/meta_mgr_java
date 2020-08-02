package com.huawei.hwcloud.gaussdb.data.store.race.utils;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.BUCKET_SIZE;

public class Util {

    public static final int index(long key) {
        return  Math.abs(Long.hashCode(key)) % BUCKET_SIZE;
    }

    public static String mem() {
        Runtime run = Runtime.getRuntime();
        long max = run.maxMemory() / 1024 / 1024;
        long total = run.totalMemory() / 1024 / 1024;
        long free = run.freeMemory() / 1024 / 1024;
        long usable = max - total + free;
        return ("Max = " + max + "M Total = " + total + "M free = " + free + "M Usable = " + usable + "M");
    }

}
