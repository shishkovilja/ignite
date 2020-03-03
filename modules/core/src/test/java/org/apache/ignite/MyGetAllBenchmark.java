package org.apache.ignite;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class MyGetAllBenchmark extends GridCommonAbstractTest {
    /** Region size. */
    public static final long REGION_SIZE = 10L << 30;

    /** Caches count. */
    private static final int CACHES_CNT = 5;

    /** Cache size. */
    private static final int CACHE_SIZE = (int)(REGION_SIZE / CACHES_CNT / (32 * 3));
    /** Report file. */
    private static File reportFile;
    /** Clients. */
    private List<IgniteClient> clients;
    /** Client caches. */
    private List<ClientCache<String, Integer>> clientCaches;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
//        cleanPersistenceDir();

        prepareReportFile();

        IgniteEx grid = startGrid(0);

        grid.cluster().state(ClusterState.ACTIVE);

        // TODO Change this???
        for (int i = 0; i < CACHES_CNT; i++) {
            if (grid.cache(DEFAULT_CACHE_NAME + i).size() < CACHE_SIZE) {
                fillCaches();

                break;
            }
        }

        // Pop pages from PDS to memory
        IdleVerifyResultV2 result = idleVerify(grid, grid.cacheNames().toArray(new String[CACHES_CNT]));

        assertFalse("Idle verify results have errors", result.hasConflicts());

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

//        cleanPersistenceDir();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        clients = new ArrayList<>(CACHES_CNT);
        clientCaches = new ArrayList<>(CACHES_CNT);

        for (int i = 0; i < CACHES_CNT; i++) {
            IgniteClient client = Ignition.startClient(getClientConfiguration());

            clients.add(client);

            clientCaches.add(client.cache(DEFAULT_CACHE_NAME + i));
        }

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try {
            super.afterTest();
        }
        finally {
            log.warning(">>>>>> Closing clients...");

            for (IgniteClient client : clients) {

                try {
                    client.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private ClientConfiguration getClientConfiguration() {
        return new ClientConfiguration()
            .setAddresses(Config.SERVER);
    }

    private void fillCaches() {
        log.warning(">>>>>> Filling caches...");

        for (int i = 0; i < CACHES_CNT; i++) {
            try (IgniteDataStreamer<String, Integer> streamer =
                     grid(0).dataStreamer(DEFAULT_CACHE_NAME + i)) {

                for (int j = 0; j < CACHE_SIZE; j++)
                    streamer.addData(Integer.toString(j), j);

                log.info(">>>>>> Cache filled: " + DEFAULT_CACHE_NAME + i);
            }

            assertEquals("Cache size should be equal to " + CACHE_SIZE,
                CACHE_SIZE,
                grid(0)
                    .cache(DEFAULT_CACHE_NAME + i)
                    .size(CachePeekMode.PRIMARY));
        }

        log.warning(">>>>>> Filling finished.");
    }

    private void prepareReportFile() throws IgniteCheckedException {
        try {
            reportFile = new File(Paths.get(U.defaultWorkDirectory()).resolve("report.csv").toUri());

            if (!reportFile.exists()) {
                log.info(">>>>>> Creating report file");

                Files.createFile(reportFile.toPath());

                try (PrintWriter writer = new PrintWriter(new FileOutputStream(reportFile), true)) {
                    writer.println("Iteration;With misses;Cache;Elapsed;Batch size;Obtained size");
                }
            }
        }
        catch (IOException e) {
            log.error("Error creating reports file", e);

            fail();
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        DataRegionConfiguration drCfg = dsCfg.getDefaultDataRegionConfiguration();

        drCfg.setPersistenceEnabled(true);
        drCfg.setInitialSize(REGION_SIZE);
        drCfg.setMaxSize(REGION_SIZE);

        BinaryConfiguration binCfg = new BinaryConfiguration().setCompactFooter(true);

        cfg.setDataStorageConfiguration(dsCfg);
        cfg.setBinaryConfiguration(binCfg);
        cfg.setCacheConfiguration(getCacheConfigurations());
        cfg.setGridLogger(new NullLogger());

        return cfg;
    }

    private CacheConfiguration<String, Integer>[] getCacheConfigurations() {
        CacheConfiguration[] cacheCfgs = new CacheConfiguration[CACHES_CNT];

        for (int i = 0; i < CACHES_CNT; i++) {
            CacheConfiguration<String, Integer> cCfg = new CacheConfiguration<String, Integer>()
                .setName(DEFAULT_CACHE_NAME + i)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setCacheMode(CacheMode.PARTITIONED);

            cacheCfgs[i] = cCfg;
        }

        return (CacheConfiguration<String, Integer>[])cacheCfgs;
    }

    /**
     * Check batches without misses, i.e. all requested keys are in the caches
     */
    @Test
    public void testBatchWithoutMisses() {
        doBatchCheck(false);
    }

    /**
     * Check batches with misses, i.e. for half of cache requested keys are <em>missed approximately in 2/3 times</em>
     */
    @Test
    public void testBatchWithMisses() {
        doBatchCheck(true);
    }

    /**
     * @param withMisses With misses.
     */
    private void doBatchCheck(boolean withMisses) {
        final int GETS_CNT = 1000;
        final int POW = 5;

        int batchSize = 10;

        try (PrintWriter writer = new PrintWriter(new FileWriter(reportFile, true), true)) {
            for (int i = 0; i < POW; i++) {
                log.info(String.format(">>>>>> Test: [batchSize=%d, getsCnt=%d]", batchSize, GETS_CNT));

                for (int j = 0; j < GETS_CNT; j++) {
                    int usedKeysCnt = batchSize * GETS_CNT;
                    Set<Integer> usedKeys = new HashSet<>(usedKeysCnt);

                    for (int k = 0; k < CACHES_CNT; k++) {
                        Set<Integer> randomKeys0 = getRandomKeys(usedKeys, batchSize,
                            withMisses && k > CACHES_CNT / 2);

                        Set<String> randomKeys = randomKeys0
                            .stream()
                            .map(Object::toString)
                            .collect(Collectors.toSet());

                        ClientCache<String, Integer> cache = clientCaches.get(k);

                        long started = System.nanoTime();

                        Map<String, Integer> resultMap = cache.getAll(randomKeys);

                        long elapsed = System.nanoTime() - started;

                        writer.printf("%d;%b;%s;%f;%d;%d\n",
                            System.currentTimeMillis(),
                            withMisses,
                            cache.getName(),
                            (double)elapsed / 1000_000,
                            batchSize,
                            resultMap.size());
                    }
                }

                batchSize *= 4;
            }
        }
        catch (IOException e) {
            log.error("Error while handling reports file", e);

            fail();
        }
    }

    /**
     * @param usedKeys Used keys.
     * @param batchSize Batch size.
     * @param withMisses Get existing keys or not (with cache "misses")
     */
    private Set<Integer> getRandomKeys(Set<Integer> usedKeys, int batchSize, boolean withMisses) {
        Set<Integer> randomKeys = new HashSet<>(batchSize);

        Random rnd = new Random();

        while (randomKeys.size() < batchSize) {
            int size = withMisses ? CACHE_SIZE * 3 : CACHE_SIZE;

            int i = rnd.nextInt(size);

            if (!usedKeys.contains(i))
                randomKeys.add(i);
        }

        assertEquals("Batch keyset isn't full", batchSize, randomKeys.size());

        usedKeys.addAll(randomKeys);

        return randomKeys;
    }
}
