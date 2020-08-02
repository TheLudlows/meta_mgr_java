package com.huawei.hwcloud.gaussdb.data.store.race.vo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@lombok.Data
@AllArgsConstructor
@Builder
public class Data implements Serializable {

    private long key;

    private long version;

    private long[] field;


    public Data(long k, long v) {
        this.key = k;
        this.version = v;
        this.field = new long[64];
    }

    public Data() {
        this.field = new long[64];
    }

    public void reset() {
        for (int i=0;i<64;i++) {
            field[i] = 0;
        }
    }


}
