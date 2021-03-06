package com.huawei.hwcloud.gaussdb.data.store.race;

import java.util.Arrays;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.page_field_num;

/**
 * as List<version-off>
 */
public class Versions {
    protected int[] vs;
    protected int[] off;
    protected byte size;
    protected int cachePosition;
    //protected long[] filed;

    public Versions(int maxSize) {
        this.size = 0;
        vs = new int[maxSize];
        off = new int[1];
        cachePosition = -1;
    }

    public static void main(String[] args) {
        Versions v = new Versions(3);
        for (int i = 0; i < 20; i++) {
            v.add(i, i);
        }

        System.out.println(v);
    }

    public void add(int v, int index) {
        int maxSize = vs.length;
        if (size == maxSize) {
            //resize
            maxSize += 2;
            int[] tempVS = new int[maxSize];
            System.arraycopy(vs, 0, tempVS, 0, vs.length);
            vs = tempVS;
        }
        if (size / page_field_num + 1 > off.length) {
            int[] newOff = new int[off.length + 1];
            System.arraycopy(off, 0, newOff, 0, off.length);
            off = newOff;
            off[size / page_field_num] = index;
        }
        if (size % page_field_num == 0) {
            off[size / page_field_num] = index;
        }
        vs[size++] = v;

    }

    @Override
    public String toString() {
        return "Versions{" +
                "vs=" + Arrays.toString(vs) +
                ", off=" + Arrays.toString(off) +
                ", count=" + size +
                '}';
    }

    public boolean needAlloc() {
        return size % page_field_num == 0;
    }
}
