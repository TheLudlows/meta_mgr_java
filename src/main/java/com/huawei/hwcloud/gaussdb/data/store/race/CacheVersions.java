package com.huawei.hwcloud.gaussdb.data.store.race;

import java.util.Arrays;

/**
 * as List<version-off>
 */
public class CacheVersions {
    private static final int default_size = 4;
    protected long key;
    protected int[] vs;
    protected long[][] field;
    protected int size;

    public CacheVersions() {
        this.size = 0;
        vs = new int[default_size];
        this.field = new long[default_size][];
        for (int i = 0; i < default_size; i++) {
            this.field[i] = new long[64];
        }
    }

    public static void main(String[] args) {
        CacheVersions v = new CacheVersions();
        v.addV(1);
        v.addV(2);
        v.addV(3);
        v.addV(4);
        v.addV(4);
        System.out.println(v);
    }

    public int addV(int v) {
        int maxSize = vs.length;
        if (size == maxSize) {
            //resize
            maxSize += 2;
            int[] tempVS = new int[maxSize];
            System.arraycopy(vs, 0, tempVS, 0, size);
            long[][] tempField = new long[maxSize][];
            System.arraycopy(this.field, 0, tempField, 0, size);
            for (int i = size; i < maxSize; i++) {
                tempField[i] = new long[64];
            }
            this.vs = tempVS;
            this.field = tempField;
        }
        this.vs[size] = v;
        return size++;
    }

    public void reset() {
        this.size = 0;
    }

    @Override
    public String toString() {
        return "CacheVersions{" +
                "key=" + key +
                ", vs=" + Arrays.toString(vs) +
                ", field=" + Arrays.toString(field) +
                ", size=" + size +
                '}';
    }
}
