package com.huawei.hwcloud.gaussdb.data.store.race;

import java.util.Arrays;

/**
 * as List<version-off>
 */
public class Versions {
    protected int[] vs;
    protected int[] off;
    protected int size;
    public Versions(int maxSize) {
        this.size = 0;
        vs = new int[maxSize];
        off = new int[maxSize];
    }

    public static void main(String[] args) {
        Versions v = new Versions(3);
        v.add(1, 1);
        v.add(1, 2);
        v.add(1, 3);
        v.add(1, 4);
        System.out.println(v);
    }

    public void add(int v, int index) {
        int maxSize = vs.length;
        if (size == maxSize) {
            //resize
            maxSize += 2;
            int[] tempVS = new int[maxSize];
            System.arraycopy(vs, 0, tempVS, 0, size);

            int[] tempOff = new int[maxSize];
            System.arraycopy(off, 0, tempOff, 0, size);

            vs = tempVS;
            off = tempOff;
        }
        vs[size] = v;
        off[size++] = index;
    }

    @Override
    public String toString() {
        return "Versions{" +
                "vs=" + Arrays.toString(vs) +
                ", off=" + Arrays.toString(off) +
                ", count=" + size +
                '}';
    }
}
