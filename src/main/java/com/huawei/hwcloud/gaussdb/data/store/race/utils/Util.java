package com.huawei.hwcloud.gaussdb.data.store.race.utils;

import java.util.concurrent.atomic.AtomicInteger;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.BUCKET_SIZE;

public class Util {

    public static final String LOG_PREFIX = "[LIBMETA_MGR] ";

    public static final AtomicInteger LOG_COUNT = new AtomicInteger(5000);
    public static final Runtime run = Runtime.getRuntime();

    public static final int index(long key) {
        return Math.abs(Long.hashCode(key)) % BUCKET_SIZE;
    }

    public static String mem() {
        long max = run.maxMemory() / 1024 / 1024;
        long total = run.totalMemory() / 1024 / 1024;
        long free = run.freeMemory() / 1024 / 1024;
        long usable = max - total + free;
        return ("Max = " + max + "M Total = " + total + "M free = " + free + "M Usable = " + usable + "M");
    }

    public static final void LOG(String s) {
        if (LOG_COUNT.decrementAndGet() > 0)
            System.out.println(LOG_PREFIX + s);
    }

    public static final void LOG_ERR(String s, Throwable e) {
        if (LOG_COUNT.decrementAndGet() > 0) {
            System.out.print(LOG_PREFIX + s);
            e.printStackTrace();
        }
    }

}
