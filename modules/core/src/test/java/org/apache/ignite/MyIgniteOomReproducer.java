package org.apache.ignite;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;
import static org.apache.ignite.configuration.WALMode.FSYNC;

/**
 *
 */
public class MyIgniteOomReproducer extends GridCommonAbstractTest {
    /** Item payload. */
    public static final byte[] PAYLOAD = new byte[DFLT_PAGE_SIZE - 120];

    /** First cache. */
    private static final String FIRST_CACHE = "cache1";

    /** Second cache. */
    private static final String SECOND_CACHE = "cache2";

    /** Cache size in bytes. */
    private static final long CACHE_SIZE = 512L << 20;

    /** Items count in partition. */
    private static final int ITEMS_CNT = (int)(CACHE_SIZE / DFLT_PAGE_SIZE);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid(0)
            .cluster()
            .active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

//        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setPageSize(DFLT_PAGE_SIZE)
            .setWalMode(FSYNC);

        dsCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setInitialSize(CACHE_SIZE / 2)
            .setMaxSize(CACHE_SIZE / 2);

        CacheConfiguration<Long, byte[]> cCfg1 = getCacheConfiguration(FIRST_CACHE);
        CacheConfiguration<Long, byte[]> cCfg2 = getCacheConfiguration(SECOND_CACHE);

        return cfg.setDataStorageConfiguration(dsCfg)
            .setCacheConfiguration(cCfg1, cCfg2)
            .setGridLogger(new NullLogger());
    }

    /**
     * @param cacheName Cache name.
     */
    private CacheConfiguration<Long, byte[]> getCacheConfiguration(String cacheName) {
        return new CacheConfiguration<Long, byte[]>()
            .setCacheMode(CacheMode.PARTITIONED)
            .setName(cacheName)
            .setGroupName("group")
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
    }

    /**
     *
     */
    @Test
    public void test() throws Exception {
        IgniteEx client = startGrid(getConfiguration().setClientMode(true));

        log.info(">>>>>> Start filling first cache...");

        try (IgniteDataStreamer<Integer, byte[]> dataStreamer = client.dataStreamer(FIRST_CACHE)) {
            for (int i = 0; i < ITEMS_CNT; i++)
                dataStreamer.addData(i, PAYLOAD);
        }

        log.info(">>>>>> Start filling second cache...");

        try (IgniteDataStreamer<Integer, byte[]> dataStreamer = client.dataStreamer(SECOND_CACHE)) {
            for (int i = 0; i < 100; i++)
                dataStreamer.addData(i, PAYLOAD);
        }

        forceCheckpoint(grid(0));

        log.info(">>>>>> Filling finished, perform first cache destroy...");

        client.destroyCache(FIRST_CACHE);

        forceCheckpoint(grid(0));

        log.info(">>>>>> Cache destroyed...");
    }
}
