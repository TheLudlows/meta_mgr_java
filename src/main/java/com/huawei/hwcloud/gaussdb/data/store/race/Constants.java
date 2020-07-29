package com.huawei.hwcloud.gaussdb.data.store.race;

public interface Constants {
    int WAL_SIZE = 32;
    int BUCKET_SIZE = 16;
    int FILED_MAPPED_SIZE = 1024 * 1024 * 1024;
    String DATA_SUFFIX = ".data";

    String LOG_PREFIX = "[LIBMETA_MGR]";

    static void LOG(String s) {
        System.out.println(LOG_PREFIX + s);
    }
}
