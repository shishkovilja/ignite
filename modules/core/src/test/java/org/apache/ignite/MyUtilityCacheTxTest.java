package org.apache.ignite;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheUtilityKey;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;
import static org.apache.ignite.configuration.WALMode.FSYNC;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Tests TX absence on clients connect and disconnect.
 */
public class MyUtilityCacheTxTest extends GridCommonAbstractTest {
    /**
     * Servers count.
     */
    public static final int SERVERS_CNT = 2;

    /**
     * Clients count.
     */
    public static final int CLIENTS_CNT = 2;

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGridsMultiThreaded(SERVERS_CNT)
            .cluster()
            .active(true);
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

//        resetLog4j(DEBUG, true, "org.apache.ignite");
    }

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration().setPageSize(DFLT_PAGE_SIZE)
            .setWalMode(FSYNC);

        dsCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(true);

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /**
     *
     */
    @Test
    public void checkSimpleStartAndShudown() {
        List<IgniteInternalFuture<IgniteEx>> futs = new ArrayList<>();

        for (int i = 0; i < CLIENTS_CNT; i++)
            futs.add(runAsync(() -> startGrid(getClientConfiguration())));

        while (futs.stream().filter(IgniteInternalFuture::isDone).count() < CLIENTS_CNT) {
            for (int i = 0; i < SERVERS_CNT; i++) {
                assertTrue("Transactions detected on grid" + i + "  while starting clients!",
                    grid(i).context().cache().context().tm().activeTransactions().isEmpty());

                checkUtilityCache(i);
            }
        }

        log().info(">>>>>> No transactions detected while starting clients, utility cache is empty.\t[OK]");
        log().info(">>>>>> Started clients:\n>>>>>>>>>" +
            grid(0).cluster()
                .forClients()
                .nodes()
                .stream()
                .map(Objects::toString)
                .collect(Collectors.joining("\n\t\t>>>>>>>>> ")));

        runAsync(() -> stopAllClients(false));

        while (!grid(0).cluster().forClients().nodes().isEmpty()) {
            for (int i = 0; i < SERVERS_CNT; i++) {
                assertTrue("Transactions detected on grid" + i + " while stopping clients!",
                    grid(i).context().cache().context().tm().activeTransactions().isEmpty());

                checkUtilityCache(i);
            }

        }

        log().info(">>>>>> All clients stopped. No transactions detected, utility cache is empty.\t[OK]");
    }

    /**
     * @param idx Index.
     */
    private void checkUtilityCache(int idx) {
        IgniteInternalCache<GridCacheUtilityKey, Object> utilityCache = grid(idx).utilityCache();

        assertNotNull("Utility cache of grid" + idx + " is null!", utilityCache);
        assertEquals("Expected that utility cache  of grid" + idx + " is empty!", 0, utilityCache.size());
    }

    /**
     *
     */
    private IgniteConfiguration getClientConfiguration() throws Exception {
        return super.getConfiguration("CLIENT_" + UUID.randomUUID().toString())
            .setClientMode(true);
    }
}
