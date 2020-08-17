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
        // update
        for(int i=0;i<size;i++) {
            if(vs[i] == v) {
                off[i] = index;
            }
            return;
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

    public long maxVersion() {
        long max = 0;
        for (int i = 0; i < size; i++) {
            if (max < vs[i]) {
                max = vs[i];
            }
        }
        return max;
    }

    public boolean allMatch(long version) {
        int match = 0;
        for (int i = 0; i < size; i++) {
            if (version >= vs[i]) {
                match++;
            }
        }
        return match == size;
    }

    public int lastLarge(long l) {
        int to = this.size - 1;
        while (0 <= to) {
            if (vs[to] <= l) {
                return to;
            }
            to--;
        }
        return 0;
    }
}
