package com.huawei.hwcloud.gaussdb.data.store.race;

public interface Constants {

    int BUCKET_OPACITY=256*1024*1024;

    int BUCKET_SIZE = 40;
    /**
     * 监控时间
     */
    int MONITOR_TIME = 5000;
    /**
     * 默认数组长度
     * map<key,<[DEFAULT_SIZE]>
     */
    int DEFAULT_SIZE = 2;

    int page_field_num = 4;
    int page_size = 64 * 8 * page_field_num;

    /**
     * 缓存比例
     */
    int cache_per = 3;
}
