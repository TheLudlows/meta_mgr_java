package com.huawei.hwcloud.gaussdb.data.store.race.vo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Arrays;

@lombok.Data
@AllArgsConstructor
@Builder
public class Data implements Serializable {

    private long key;

    private long version;

    private long[] field;

    public Data() {
    }

    public Data(int size) {
        this.field = new long[size];
    }

    public void reset() {
        for (int i=0;i<64;i++) {
            field[i] = 0;
        }
    }

    @Override
    public String toString() {
        return "Data{" +
                "key=" + key +
                ", version=" + version +
                ", field=" + Arrays.toString(field) +
                '}';
    }
}
