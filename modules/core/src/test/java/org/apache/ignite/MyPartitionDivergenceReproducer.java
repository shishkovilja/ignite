package org.apache.ignite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.verify.CacheFilterEnum;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskArg;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskV2;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class MyPartitionDivergenceReproducer extends GridCommonAbstractTest {
    /** Test cache. */
    private static final String TEST_CACHE = "TEST";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Integer, String> cCfg = new CacheConfiguration<>();
        cCfg.setName(TEST_CACHE);
        cCfg.setCacheMode(CacheMode.PARTITIONED);
        cCfg.setBackups(1);
        cCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC);

        cfg.setCacheConfiguration(cCfg);

        cfg.setSslContextFactory(GridTestUtils.sslFactory());

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        DataRegionConfiguration drCfg = dsCfg.getDefaultDataRegionConfiguration();
        drCfg.setInitialSize(512L << 20);
        drCfg.setMaxSize(512L << 30);
        drCfg.setPersistenceEnabled(false);

        return cfg;
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

    /**
     *
     */
    @Test
    public void testDivergencyWhileNodesStart() throws Exception {
        final int GRIDS_CNT = 3;

        startGridsMultiThreaded(GRIDS_CNT);

        for (int i = 0; i < GRIDS_CNT; i++)
            assertTrue(grid(i).context().cache().context().tm().activeTransactions().isEmpty());

        checkIdleVerify(GRIDS_CNT);
    }

    /**
     *
     */
    @Test
    public void testDivergencyWithTxWhileNodesStart_noWaitForTx() throws Exception {
        doTxWhileNodesStart(false);
    }

    /**
     *
     */
    @Test
    public void testDivergencyWithTxWhileNodesStart_withWaitForTx() throws Exception {
        doTxWhileNodesStart(true);
    }

    /**
     * @param waitForTx Wait for topology.
     */
    private void doTxWhileNodesStart(boolean waitForTx) throws Exception {
        final int INIT_GRIDS_CNT = 3;
        final int EXTRA_GRIDS_CNT = 2;

        startGridsMultiThreaded(INIT_GRIDS_CNT);

        checkIdleVerify(INIT_GRIDS_CNT);

        waitForTopology(INIT_GRIDS_CNT);

        List<IgniteInternalCache<Object, Object>> utilityCaches = new ArrayList<>(INIT_GRIDS_CNT);
        for (int i = 0; i < INIT_GRIDS_CNT; i++)
            utilityCaches.add(grid(i).context().cache().utilityCache());

        IgniteInternalFuture<?> txFut = runAsync(() -> {
            try (final Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                for (int i = 0; i < INIT_GRIDS_CNT; i++) {
                    for (int j = 0; j < 1000; j++) {
                        IgniteInternalCache<Object, Object> cache = utilityCaches.get(i);

                        cache.put(Integer.toString(i * 1000 + j), UUID.randomUUID().toString());
                    }
                }

                waitForTopology(INIT_GRIDS_CNT + EXTRA_GRIDS_CNT);

                doSleep(1000);

                tx.commit();
            }
            catch (IllegalStateException | IgniteCheckedException e) {
                log.error("Utitlity cache tx failed: ", e);

                fail();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });

        startGridsMultiThreaded(INIT_GRIDS_CNT, EXTRA_GRIDS_CNT);

        if (waitForTx)
            txFut.get();

        checkIdleVerify(INIT_GRIDS_CNT + EXTRA_GRIDS_CNT);
    }

    /**
     * @param serversCnt Servers count.
     */
    private void checkIdleVerify(int serversCnt) {
        log.info(">>>>>> Idle verify started: serversCnt=" + serversCnt);

        for (int i = 0; i < serversCnt; i++) {
            log.info(">>>>>> Starting idle verify for grid#" + i);
            IdleVerifyResultV2 idleVerifyResult = idleVerifyUtilityCache(grid(i));
            idleVerifyResult.print(log::warning);

            assertFalse("Idle verify has conflicts on grid#" + i, idleVerifyResult.hasConflicts());
            assertTrue("Idle verify failed on grid#" + i, idleVerifyResult.exceptions().isEmpty());
        }
    }

    /**
     * @param grid Grid.
     */
    private IdleVerifyResultV2 idleVerifyUtilityCache(IgniteEx grid) {
        Set<String> cacheNames = new HashSet<>();

        cacheNames.add(GridCacheUtils.UTILITY_CACHE_NAME);

        VisorIdleVerifyTaskArg taskArg = new VisorIdleVerifyTaskArg(
            cacheNames,
            Collections.emptySet(),
            false,
            CacheFilterEnum.SYSTEM,
            false);

        return grid.compute().execute(
            VisorIdleVerifyTaskV2.class.getName(),
            new VisorTaskArgument<>(grid.localNode().id(), taskArg, false)
        );
    }
}
