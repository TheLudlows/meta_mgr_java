package test;

import com.carrotsearch.hppc.LongObjectHashMap;

import java.util.ArrayList;
import java.util.List;

public class MapTest {
    public static void main(String[] args) {
        LongObjectHashMap<List<Integer>> map = new LongObjectHashMap<>();
        List list = map.getOrDefault(1,new ArrayList<>());
        list.add(1);
        System.out.println(map);
    }
}
