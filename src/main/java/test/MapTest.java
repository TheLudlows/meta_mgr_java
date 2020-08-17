package test;

public class MapTest {
    public static void main(String[] args) throws Throwable {
        long a = Integer.MAX_VALUE*4L;
        System.out.println(Long.bitCount(a));

        long l = Integer.MAX_VALUE;
        l = l*6;
        System.out.println(l);
    }
}
