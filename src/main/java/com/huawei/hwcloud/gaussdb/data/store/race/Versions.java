package com.huawei.hwcloud.gaussdb.data.store.race;

import java.util.Arrays;

public class Versions {
    protected long[] vs;
    protected int[] off;
    protected int size;
    protected int maxSize;

    public void add(long v, int index) {
        if (size == maxSize) {
            //resize
            maxSize += 2;
            long[] tempVS = new long[maxSize];
            System.arraycopy(vs,0,tempVS,0,size);

            int[] tempOff = new int[maxSize];
            System.arraycopy(off,0,tempOff,0,size);

            vs = tempVS;
            off = tempOff;
        }
        vs[size] = v;
        off[size++] = index;
    }

    public Versions(int maxSize) {
        this.size = 0;
        this.maxSize = maxSize;
        vs = new long[maxSize];
        off = new int[maxSize];
    }

    @Override
    public String toString() {
        return "Versions{" +
                "vs=" + Arrays.toString(vs) +
                ", off=" + Arrays.toString(off) +
                ", count=" + size +
                ", size=" + maxSize +
                '}';
    }

    public static void main(String[] args) {
        Versions v = new Versions(3);
        v.add(1,1);
        v.add(1,2);
        v.add(1,3);
        v.add(1,4);
        System.out.println(v);
    }

}
