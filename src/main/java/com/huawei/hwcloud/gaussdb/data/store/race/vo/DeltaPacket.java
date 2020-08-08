package com.huawei.hwcloud.gaussdb.data.store.race.vo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DeltaPacket implements Serializable {

    private static final long serialVersionUID = 4750719966645557615L;

    private long version;

    private long deltaCount;

    private List<DeltaItem> deltaItem;

    @Override
    public String toString() {
        return "DeltaPacket{" +
                "version=" + version +
                ", deltaCount=" + deltaCount +
                ", deltaItem=" + deltaItem +
                '}';
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class DeltaItem implements Serializable {

        private static final long serialVersionUID = 4750719966645557615L;

        private long key;

        private long[] delta;

        @Override
        public String toString() {
            return "DeltaItem{" +
                    "key=" + key + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DeltaItem deltaItem = (DeltaItem) o;
            return key == deltaItem.key;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(key);
        }

        public void add(long[] f) {
            for(int i=0;i<64;i++) {
                this.delta[i] += f[i];
            }
        }
    }


}
