package org.apache.ignite;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class MyBenchmarkThick extends GridCommonAbstractTest {
    /** Servers count. */
    public static final int SERVERS_CNT = 2;

    /** Clients count. */
    public static final int CLIENTS_CNT = 2;

    /** Tasks count. */
    public static final int TASKS_CNT = 16;

    /** Items count. */
    private static final int ITEMS_CNT = 3_000_000;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(SERVERS_CNT);
        startClientGrid(CLIENTS_CNT);

        awaitPartitionMapExchange();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try {
            super.afterTest();
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (!cfg.isClientMode()) {
            CacheConfiguration<String, byte[]> cacheCfg = new CacheConfiguration<String, byte[]>()
                .setName(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(1);

            DataStorageConfiguration dsCfg = new DataStorageConfiguration();

            dsCfg.getDefaultDataRegionConfiguration()
                .setInitialSize(ITEMS_CNT * 2000L)
                .setMaxSize(ITEMS_CNT * 2000L)
                .setPersistenceEnabled(false);

            cfg.setCacheConfiguration(cacheCfg);
            cfg.setDataStorageConfiguration(dsCfg);
        }

        cfg.setGridLogger(new NullLogger());

        return cfg;
    }

    /**
     *
     */
    @Test
    public void runBenchmark() {
        ClusterGroup clients = grid(0).cluster().forClients();

        ExecutorService srvc = grid(0).executorService(clients);

        List<Future<?>> futs = new ArrayList<>(TASKS_CNT);

        final long started = System.currentTimeMillis();

        for (int i = 0; i < TASKS_CNT; i++) {
            final Future<?> fut = srvc.submit(
                new IgniteRunnable() {
                    @IgniteInstanceResource
                    Ignite ignite;

                    @Override public void run() {
                        final ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        try (final IgniteDataStreamer<String, byte[]> streamer =
                                 ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
                            final int TOTAL = ITEMS_CNT / TASKS_CNT;

                            int i = 0;

                            while (i++ <= TOTAL)
                                streamer.addData(UUID.randomUUID().toString(), randomPayload(rnd));

                            log.info(">>>>>> Filling finished.");
                        }
                        catch (Exception e) {
                            log.error(">>>>>> Error with thin client: ", e);

                            fail();
                        }
                    }

                    private byte[] randomPayload(ThreadLocalRandom rnd) {
                        final int sz = 500 + rnd.nextInt(1000);

                        byte[] payload = new byte[sz];
                        rnd.nextBytes(payload);
                        return payload;
                    }
                }
            );

            futs.add(fut);
        }

        try {
            for (Future<?> fut : futs)
                fut.get();
        }
        catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            log.error("Future error: ", e);

            fail();
        }

        final long elapsed = System.currentTimeMillis() - started;

        final int cacheSz = grid(0).cache(DEFAULT_CACHE_NAME).size(CachePeekMode.PRIMARY);

        assertTrue("Not enough items in cache", cacheSz >= ITEMS_CNT);

        log.info(String.format(">>>>>> Finished: [elapsed=%d, tps=%.2f]",
            elapsed, ((double)cacheSz) / elapsed * 1000));
    }
}
