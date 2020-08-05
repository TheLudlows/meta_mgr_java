package com.huawei.hwcloud.gaussdb.data.store.race.utils;


public class DyArray {
    private static final int DEFAULT_SIZE = 32;

    public static int[] newArray() {
        return new int[32];
    }

    public static int[] add(int[] arr, long version, int index) {
        int size = arr[0];
        int length = arr.length;
        if (length - size < 3) {
            // resize
            int[] newArr = new int[length + 6];
            System.arraycopy(arr, 0, newArr, 0, length);
            arr = newArr;
        }
        size = size * 3;
        arr[++size] = (int) (version & 0x00000000ffffffffL); //低32位
        arr[++size] = (int) (version >> 32); //高32位
        arr[++size] = index;
        arr[0] += 1;
        return arr;
    }

    public static long getVersion(int i, int[] arr) {
        if (i * 3 + 1 > arr.length) {
            throw new IndexOutOfBoundsException("arr" + i * 3 + 1);
        }
        return arr[i * 3 + 1] | arr[i * 3 + 2] << 32L;
    }

    public static long getIndex(int i, int[] arr) {
        if (i * 3 + 1 > arr.length) {
            throw new IndexOutOfBoundsException("arr" + i * 3 + 1);
        }
        return arr[i * 3 + 3];
    }

    public static void main(String[] args) {
        int[] arr = newArray();
        add(arr, 1, 2);
        add(arr, 2, 4);

        System.out.println(arr[0]);
        System.out.println(getVersion(0, arr));
        System.out.println(getVersion(1, arr));
        System.out.println(getIndex(0, arr));
        System.out.println(getIndex(1, arr));

    }

}
