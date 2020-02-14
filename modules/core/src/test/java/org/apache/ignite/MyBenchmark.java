package org.apache.ignite;

import java.util.UUID;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class MyBenchmark extends GridCommonAbstractTest {
    /** Servers count. */
    public static final int SERVERS_CNT = 2;

    /** Clients count. */
    public static final int CLIENTS_CNT = 16;

    /** Items count. */
    private static final int ITEMS_CNT = 1_000_000;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(SERVERS_CNT);

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

        CacheConfiguration<String, byte[]> cacheCfg = new CacheConfiguration<String, byte[]>()
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.getDefaultDataRegionConfiguration()
            .setInitialSize(ITEMS_CNT * 3000L)
            .setMaxSize(ITEMS_CNT * 3000L)
            .setPersistenceEnabled(false);

        cfg.setCacheConfiguration(cacheCfg);
        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /**
     *
     */
    @Test
    public void runBenchmark() throws Exception {
        byte[] PAYLOAD = new byte[1500];

        GridTestUtils.runMultiThreaded(() -> {
                try (IgniteClient client = Ignition.startClient(getClientConfiguration())) {
                    ClientCache<String, byte[]> cache = client.cache(DEFAULT_CACHE_NAME);

                    for (int i = 0; i < ITEMS_CNT / CLIENTS_CNT; i++)
                        cache.put(UUID.randomUUID().toString(), PAYLOAD);

                    log.info(">>>>>> Filling finished.");
                }
                catch (Exception e) {
                    log.error(">>>>>> Error with thin client: ", e);

                    fail();
                }
            },
            CLIENTS_CNT,
            "thin-thread"
        );

        assertEquals(ITEMS_CNT, grid(0).cache(DEFAULT_CACHE_NAME).size(CachePeekMode.PRIMARY));
    }

    /**
     *
     */
    private ClientConfiguration getClientConfiguration() {
        return new ClientConfiguration()
            .setAddresses("127.0.0.1:10800");
    }
}
