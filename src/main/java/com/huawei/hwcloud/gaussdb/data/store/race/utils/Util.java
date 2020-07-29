package com.huawei.hwcloud.gaussdb.data.store.race.utils;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

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
}
