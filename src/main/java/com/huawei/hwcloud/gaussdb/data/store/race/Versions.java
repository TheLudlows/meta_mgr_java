package com.huawei.hwcloud.gaussdb.data.store.race;

import java.util.Arrays;

public class Versions {
    private static final int BLOCK_SIZE = 8;
    private static final int ID_SIZE = 9;

    protected long[] versions;
    protected int size;
    protected long[] filed;

    public void add(long v, int blockId) {

        if (size % ID_SIZE == 0) {
            // 扩容
            versions = Arrays.copyOf(versions, versions.length * 2);
            versions[size++] = blockId;
            versions[size++] = v;
            return;
        }
        versions[size++] = v;
    }

    public Versions() {
        this.size = 0;
        versions = new long[BLOCK_SIZE + 1];
    }

    public void addField(long[] l) {
        if (filed == null) {
            filed = new long[64];
        }
        for (int i = 0; i < 64; i++) {
            filed[i] += l[i];
        }
    }

    public long maxVersion() {
        long max = 0;
        for (int i = 0; i < size; i++) {
            if (i % BLOCK_SIZE == 0) {
                continue;
            }
            if (max < versions[i]) {
                max = versions[i];
            }
        }
        return max;
    }

    @Override
    public String toString() {
        return "Versions{" +
                "versions=" + Arrays.toString(versions) +
                ", size=" + size +
                ", filed=" + Arrays.toString(filed) +
                '}';
    }

    public static void main(String[] args) {
        Versions v = new Versions();
        v.add(1, 1);
        v.add(2, 1);
        v.add(3, 1);
        v.add(4, 1);
        v.add(5, 1);
        v.add(6, 1);
        v.add(7, 1);
        v.add(8, 1);
        v.add(9, 2);
        v.add(10, 2);

        System.out.println(v);
    }

}
