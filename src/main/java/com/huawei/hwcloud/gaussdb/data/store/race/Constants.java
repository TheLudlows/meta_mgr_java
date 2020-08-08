package com.huawei.hwcloud.gaussdb.data.store.race;

public interface Constants {
    /**
     * filed ta wal size 128kb
     */
    int WAL_SIZE = 1024 * 128;
    /**
     * wal count 4096
     */
    int WAL_COUNT = WAL_SIZE / 64 / 8;

    int BUCKET_SIZE = 48;
    /**
     * key-v wal size
     */
    int KEY_MAPPED_SIZE = WAL_COUNT * 16;
    /**
     * 监控时间
     */
    int MONITOR_TIME = 5000;
    /**
     * 默认数组长度
     * map<key,<[DEFAULT_SIZE]>
     */
    int DEFAULT_SIZE = 2;

}
