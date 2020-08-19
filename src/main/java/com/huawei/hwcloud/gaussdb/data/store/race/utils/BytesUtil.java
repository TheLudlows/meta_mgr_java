package com.huawei.hwcloud.gaussdb.data.store.race.utils;

/**
 * @author chender
 * @date 2020/6/16 10:52
 */
public class BytesUtil {

    public static void intToByteArr(int x, byte[] bytes, int startIndex) {
        bytes[startIndex + 3] = (byte) (x & 0xff);
        bytes[startIndex + 2] = (byte) (x >> 8 & 0xff);
        bytes[startIndex + 1] = (byte) (x >> 16 & 0xff);
        bytes[startIndex] = (byte) (x >> 24 & 0xff);
    }

    public static int byteArrToInt(byte[] arr, int startIndex) {
        int x = ((arr[startIndex] & 0xff) << 24) | ((arr[startIndex + 1] & 0xff) << 16) | ((arr[startIndex + 2] & 0xff) << 8) | (arr[startIndex + 3] & 0xff);
        return x;
    }

    public static void longToByteArr(long x, byte[] bytes, int startIndex) {
        bytes[startIndex + 7] = (byte) (x & 0xff);
        bytes[startIndex + 6] = (byte) (x >> 8 & 0xff);
        bytes[startIndex + 5] = (byte) (x >> 16 & 0xff);
        bytes[startIndex + 4] = (byte) (x >> 24 & 0xff);
        bytes[startIndex + 3] = (byte) (x >> 32 & 0xff);
        bytes[startIndex + 2] = (byte) (x >> 40 & 0xff);
        bytes[startIndex + 1] = (byte) (x >> 48 & 0xff);
        bytes[startIndex] = (byte) (x >> 56 & 0xff);
    }

    public static long byteArrToLong(byte[] arr, int startIndex) {
        long x = (((long) arr[startIndex] & 0xff) << 56) | (((long) arr[startIndex + 1] & 0xff) << 48)
                | (((long) arr[startIndex + 2] & 0xff) << 40) | (((long) arr[startIndex + 3] & 0xff) << 32)
                | (((long) arr[startIndex + 4] & 0xff) << 24) | (((long) arr[startIndex + 5] & 0xff) << 16)
                | (((long) arr[startIndex + 6] & 0xff) << 8) | ((long) arr[startIndex + 7] & 0xff);
        return x;
    }

    public static void main(String[] args) {
        byte[] bytes = new byte[1024];
        longToByteArr(1234567898765334L, bytes, 18);
        System.out.println(byteArrToLong(bytes, 18));
    }
}
