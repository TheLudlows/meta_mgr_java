package com.huawei.hwcloud.gaussdb.data.store.race;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class Counter {
    public static LongAdder writeCounter = new LongAdder();
    public static LongAdder readCounter = new LongAdder();
    public static LongAdder randomRead = new LongAdder();
    public static LongAdder cacheHit = new LongAdder();
    public static LongAdder totalReadSize = new LongAdder();
}
