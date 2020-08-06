package com.huawei.hwcloud.gaussdb.data.store.race;

import java.util.concurrent.atomic.LongAdder;

public class Counter {
    public static final LongAdder allMatchTimes = new LongAdder();
    public static LongAdder writeCounter = new LongAdder();
    public static LongAdder readCounter = new LongAdder();
    public static LongAdder randomRead = new LongAdder();
    public static LongAdder mergeRead = new LongAdder();
    public static LongAdder totalReadSize = new LongAdder();
}
