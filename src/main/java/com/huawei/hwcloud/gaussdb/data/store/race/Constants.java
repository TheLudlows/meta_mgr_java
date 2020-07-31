package com.huawei.hwcloud.gaussdb.data.store.race;

import java.util.concurrent.atomic.AtomicInteger;

public interface Constants {
    int WAL_SIZE = 1024 * 64;
    int WAL_COUNT = WAL_SIZE / 64 / 8;
    int BUCKET_SIZE = 32;
    int FILED_MAPPED_SIZE = 1024 * 1024 * 1024;

    boolean GLOBAL_DIO = false;
    String LOG_PREFIX = "[LIBMETA_MGR] ";

    AtomicInteger LOG_COUNT = new AtomicInteger(1000);

    static void LOG(String s) {
        if (LOG_COUNT.decrementAndGet() > 0)
            System.out.println(LOG_PREFIX + s);
    }

    static void LOG_ERR(String s, Exception e) {
        if (LOG_COUNT.decrementAndGet() > 0) {
            System.out.print(LOG_PREFIX + s);
            e.printStackTrace();
        }
    }

}
