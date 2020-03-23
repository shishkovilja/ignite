package org.apache.ignite;

import java.util.UUID;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;
import static org.apache.ignite.configuration.WALMode.FSYNC;

/**
 * Reproduces imlicit destroy of local caches with same name
 */
public class MyLocalCacheDestroyReproducer extends GridCommonAbstractTest {
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
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setPageSize(DFLT_PAGE_SIZE)
            .setWalMode(FSYNC);

        dsCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(true);

        CacheConfiguration<Integer, String> cacheCfg = new CacheConfiguration<Integer, String>(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.LOCAL);

        return cfg
            .setDataStorageConfiguration(dsCfg)
            .setCacheConfiguration(cacheCfg);
    }

    /**
     *
     */
    @Test
    public void test() throws Exception {
        final int SERVERS_CNT = 4;

        startGrids(SERVERS_CNT)
            .cluster()
            .active(true);

        // Filling caches
        for (int i = 0; i < SERVERS_CNT; i++) {
            IgniteCache<Integer, String> cache = grid(i).cache(DEFAULT_CACHE_NAME);

            for (int j = 0; j < (i + 1) * 10; j++)
                cache.put(j, UUID.randomUUID().toString());
        }

        for (int i = 0; i < SERVERS_CNT; i++) {
            assertNotNull("Cache should exist on grid#" + i, grid(i).cache(DEFAULT_CACHE_NAME));

            final int cacheSz = (i + 1) * 10;

            assertEquals("Cache size should be equal to '" + cacheSz + "' on grid#" + i,
                cacheSz, grid(i).cache(DEFAULT_CACHE_NAME).size(CachePeekMode.ALL));
        }

        printDefaultCacheInfo(SERVERS_CNT);

        // Destroy cache only on one grid
        grid(0).cache(DEFAULT_CACHE_NAME).destroy();

        awaitPartitionMapExchange();
        forceCheckpoint(G.allGrids());

        // Ensure that it was deleted
        assertNull(grid(0).cache(DEFAULT_CACHE_NAME));

        printDefaultCacheInfo(SERVERS_CNT);

        // Try to ensure, that caches was NOT deleted -> this will lead to assertion error
        for (int i = 1; i < SERVERS_CNT; i++)
            assertNotNull("Cache should exist on grid#" + i, grid(i).cache(DEFAULT_CACHE_NAME));
    }

    /**
     * @param serversCnt Servers count.
     */
    void printDefaultCacheInfo(int serversCnt) {
        for (int i = 0; i < serversCnt; i++) {
            IgniteCache<Object, Object> cache = grid(i).cache(DEFAULT_CACHE_NAME);

            int cacheSz = cache != null ? cache.size(CachePeekMode.PRIMARY) : -1;

            log.info(String.format(">>>>>> Default cache size on grid#%d: %d", i, cacheSz));
        }
    }
}
