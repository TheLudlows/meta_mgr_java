package com.huawei.hwcloud.gaussdb.data.store.race.utils;

import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.WAL_SIZE;

public class WAL {

    private MappedByteBuffer buffer;
    private FileChannel channel;
    private long address;
    private int count;

    public WAL(String fileName) {
        try {
            this.channel = FileChannel.open(new File(fileName).toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, WAL_SIZE);
            this.address = ((DirectBuffer) this.buffer).address();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write(long[] arr) {
        for (long l : arr) {
            buffer.putLong(l);
        }
        count++;
    }

    public void write(byte[] bytes) {

        buffer.put(bytes);
        count++;
    }

    public byte[] read() {
        byte[] bytes = new byte[32];
        buffer.flip();
        buffer.get(bytes, 0, 32);
        return bytes;
    }

    public static void main(String[] args) {

        WAL wal = new WAL("b.wal");
        ByteBuffer buf = wal.buffer.duplicate();
        buf.position(0);
        buf.limit(1);
        byte b = buf.get();
        System.out.println(b);
        wal.write("a".getBytes());
    }


}
