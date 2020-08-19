package com.huawei.hwcloud.gaussdb.data.store.race;

import com.huawei.hwcloud.gaussdb.data.store.race.utils.BytesUtil;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.*;

/**
 * @author chender
 * @date 2020/8/16 20:40
 */
public class CacheService {
    private static ByteBuffer cacheBuffer = ByteBuffer.allocateDirect(cache_capacity + page_size);
    private static AtomicInteger cachePosition = new AtomicInteger(-page_size / 2 - 8);
    private static volatile boolean full;

    public static void init() {
        System.gc();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public static int saveCahe(long key, int[] fields, byte[] exceed, int index, int position) {
        if (position == -1 && (index != 0 || full || (position = cachePosition.addAndGet(page_size / 2 + 8)) >= cache_capacity - page_size)) {
            full = true;
            return -1;
        }
        int base = position + 8 + index * item_size;
        if (index == 0) {
            cacheBuffer.putLong(base - 8, key);
        }
        for (int i = 0; i < 64; i++) {
            cacheBuffer.putInt(base + i * 4, fields[i]);
        }
        cacheBuffer.putLong(base + field_size, BytesUtil.byteArrToLong(exceed, 0));
        cacheBuffer.putLong(base + field_size + 8, BytesUtil.byteArrToLong(exceed, 8));
        return position;
    }

    public static void mergeCache(long key, int position, int skip, int mergeSize, ByteBuffer byteBuffer) {
        if (position % (page_size + 16) != 0) {
            return;
        }
        int base = position + item_size * skip + skip / 2 * 8;
        for (int j = 0; j < mergeSize; j++) {
            if ((skip + j) % 2 == 0) {
                cacheBuffer.putLong(base, key);
                base += 8;
            }
            for (int i = 0; i < 34; i++) {
                cacheBuffer.putLong(base, byteBuffer.getLong());
                base += 8;
            }
        }
    }


    public static boolean getCacheData(ByteBuffer byteBuffer, int position, int versionSize) {
        if (!full) {
            return false;
        }

        for (int j = 0; j < versionSize; j++) {
            int base = j * item_size + position + 8;
            for (int i = 0; i < 32; i++) {
                byteBuffer.putLong(cacheBuffer.getLong(base + i * 8));
            }
            byteBuffer.putLong(cacheBuffer.getLong(base + field_size));
            byteBuffer.putLong(cacheBuffer.getLong(base + field_size + 8));
        }
        return true;
    }

    public static boolean getCacheData(long key, int position, int versionSize, ByteBuffer byteBuffer) {
        if (!full) {
            return false;
        }
        int base = position + 8;
        for (int j = 0; j < versionSize; j++) {
            if (j >= 2 && j % 2 == 0) {
                if (cacheBuffer.getLong(base) == key) {
                    base += 8;
                } else {
                    break;
                }
            }
            for (int i = 0; i < 32; i++) {
                byteBuffer.putLong(cacheBuffer.getLong(base + i * 8));
            }
            byteBuffer.putLong(cacheBuffer.getLong(base + field_size));
            byteBuffer.putLong(cacheBuffer.getLong(base + field_size + 8));
            base += item_size;
        }
        return cacheBuffer.getLong(position) == key;
    }

}
