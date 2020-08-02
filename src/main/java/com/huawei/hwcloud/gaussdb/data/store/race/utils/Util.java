package com.huawei.hwcloud.gaussdb.data.store.race.utils;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.BUCKET_SIZE;

public class Util {
    public static final Unsafe UNSAFE = unsafe();

    public static Unsafe unsafe() {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            return (Unsafe) field.get(null);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static final int index(long key) {
        return (int) Math.abs((key ^ (key >>> 32)) % BUCKET_SIZE);
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
