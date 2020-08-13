package com.huawei.hwcloud.gaussdb.data.store.race;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class Counter {
    public static final LongAdder writeCounter = new LongAdder();
    public static final LongAdder readCounter = new LongAdder();
    public static final LongAdder randomRead = new LongAdder();
    public static final LongAdder cacheHit = new LongAdder();
    public static final LongAdder totalReadSize = new LongAdder();
    public static final AtomicInteger maxSize = new AtomicInteger();
}
