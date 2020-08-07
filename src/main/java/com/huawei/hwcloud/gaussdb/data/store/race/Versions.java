package com.huawei.hwcloud.gaussdb.data.store.race;

import java.util.Arrays;

import static com.huawei.hwcloud.gaussdb.data.store.race.Counter.allMatchTimes;

/**
 * as List<version-off>
 */
public class Versions {
    protected long[] vs;
    protected int[] off;
    protected int size;
    protected long[] filed;

    public Versions(int maxSize) {
        this.size = 0;
        vs = new long[maxSize];
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

    public void add(long v, int index) {
        int maxSize = vs.length;
        if (size == maxSize) {
            //resize
            maxSize += 2;
            long[] tempVS = new long[maxSize];
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
            if (max < vs[i]) {
                max = vs[i];
            }
        }
        return max;
    }

    public int queryFunc(long version) {
        int match = 0;
        for (int i = 0; i < size; i++) {
            if (version >= vs[i]) {
                match++;
            }
        }

        if (match == size) {
            allMatchTimes.add(1);
            if (filed == null) {// not in mem
                return match;
            }
            // all in mem
            return -1;
        } else {
            return match;
        }
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
