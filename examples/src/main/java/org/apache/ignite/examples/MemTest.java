package org.apache.ignite.examples;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;

public class MemTest {
//    public static final byte[] VALUE = new byte[1024];

    public static final byte[] VALUE = new byte[1024 * 1024];

    public static void main(String[] args) {
        Ignition.setClientMode(true);

        Ignite ignite = Ignition.start("examples/config/example-ignite.xml");

        final IgniteCache<Integer, byte[]> cache = ignite.cache("wwwplm-v1");

        putAll(cache);

//        infinitePutAlls(cache);
    }

    private static void infinitePutAlls(IgniteCache<Integer, byte[]> cache) {
        new Thread(new Runnable() {
            @Override public void run() {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Thread.sleep(1000);
                    }
                    catch (InterruptedException e) {
                        return;
                    }

                    System.out.println("Size: " + cache.size());
                }
            }
        }).start();

        final AtomicInteger key = new AtomicInteger();

        ExecutorService exec = Executors.newFixedThreadPool(4);

        for (int i = 0; i < 4; i++) {
            exec.submit(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    while (!Thread.currentThread().isInterrupted()) {
//                        Map<Integer, byte[]> map = new HashMap<>(10, 1.0f);
//
//                        for (int i = 0; i < 10; i++)
//                            map.put(key.getAndIncrement(), VALUE);
//
//                        cache.putAll(map);

                        cache.put(key.getAndIncrement(), VALUE);
                    }

                    return null;
                }
            });
        }
    }

    private static void putAll(IgniteCache<Integer, byte[]> cache) {
        final AtomicInteger key = new AtomicInteger();

        int sz = 1;

        for (int k = 0; k < sz; k++) {
            Map<Integer, byte[]> map = new HashMap<>(sz, 1.0f);

            for (int i = 0; i < sz; i++)
                map.put(key.getAndIncrement(), VALUE);

            cache.putAll(map);
        }
    }
}
