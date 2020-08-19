package com.huawei.hwcloud.gaussdb.data.store.race;

import com.huawei.hwcloud.gaussdb.data.store.race.vo.Data;
import com.huawei.hwcloud.gaussdb.data.store.race.vo.DeltaPacket;

import java.io.IOException;

/**
 * 数据分区策略
 * 1. 基于线程分区，即一个线程内所有的数据写入同一个文件。
 * 2. key 分区，key取余 相同的key落入一个文件中。
 * 进程崩溃可恢复的策略
 * 1. mapped file 缺点：多次触发缺页中断，造成写入缓慢，优点：可更新
 * 2. mapped buffer + dio
 * 3. mapped buffer + channel 缺点：相比dio方式多一次复制
 * 4. 同步写
 * <p>
 * 因此有几种不同的实现方式
 **/
public interface DBEngine {

    void init();

    void write(long v, DeltaPacket.DeltaItem item, byte[] exceed) throws IOException;

    void print();

    Data read(long key, long v) throws IOException;

    void close();
}
