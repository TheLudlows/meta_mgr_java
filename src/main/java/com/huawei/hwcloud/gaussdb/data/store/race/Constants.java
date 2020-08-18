package com.huawei.hwcloud.gaussdb.data.store.race;

public interface Constants {

    int BUCKET_SIZE = 30;
    /**
     * 监控时间
     */
    int MONITOR_TIME = 5000;
    /**
     * 默认数组长度
     * map<key,<[DEFAULT_SIZE]>
     */
    int DEFAULT_SIZE = 4;

    int page_field_num = 4;

    int field_size=64 * 4;
    int exceed_size=16;

    int item_size=field_size+exceed_size;

    int page_size = item_size * page_field_num;

    /**
     * 缓存比例
     */
    int cache_per = 3;

    int cache_capacity=1024*1024*512;
}
