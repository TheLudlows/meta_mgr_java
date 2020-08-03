package test;

import com.carrotsearch.hppc.LongObjectHashMap;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.KEY_MAPPED_SIZE;
import static com.huawei.hwcloud.gaussdb.data.store.race.Constants.LOG_ERR;
import static java.nio.file.StandardOpenOption.*;

public class MapTest {
    public static void main(String[] args) throws Throwable {
        LongObjectHashMap<List<Integer>> map = new LongObjectHashMap<>();
        List list = map.getOrDefault(1,new ArrayList<>());
        list.add(1);
        System.out.println(map);
        MappedByteBuffer byteBuffer1  = FileChannel.open(new File("a").toPath(), CREATE, READ, WRITE)
                .map(FileChannel.MapMode.READ_WRITE, 0, KEY_MAPPED_SIZE);

        MappedByteBuffer byteBuffer2  = FileChannel.open(new File("a").toPath(), CREATE, READ, WRITE)
                .map(FileChannel.MapMode.READ_WRITE, 0, KEY_MAPPED_SIZE);

       // byteBuffer1.putInt(0,1);
        int i = byteBuffer2.getInt(0);

        System.out.println(i);
    }
}
