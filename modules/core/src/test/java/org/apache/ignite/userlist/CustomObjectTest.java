package org.apache.ignite.userlist;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class CustomObjectTest extends GridCommonAbstractTest {
    private static final String CACHE = "TEST_CACHE";
    private static final String CLIENT = "CLIENT";

    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);

        super.beforeTestsStarted();
    }

    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        cleanPersistenceDir();
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        CacheConfiguration<CustomObject, String> cCfg =
            new CacheConfiguration<CustomObject, String>().setName("TEST_CACHE");

        cfg.setDataStorageConfiguration(dsCfg)
            .setCacheConfiguration(cCfg);

        cfg.setActiveOnStart(true);

        return cfg;
    }

    @Test
    public void testPut() throws Exception {
        Ignite client0 = startGrid(getClientCfg());
        client0.cluster().active(true);

        IgniteCache<CustomObject, String> cache0 = client0.cache(CACHE);

        for (int i = 0; i < 10; i++) {
            cache0.put(new CustomObject(), UUID.randomUUID().toString());
        }

        forceCheckpoint();
        assertEquals(1, cache0.size(CachePeekMode.ALL));

        stopGrid(CLIENT);

        Ignite client1 = startGrid(getClientCfg());

        IgniteCache<CustomObject, String> cache1 = client1.cache(CACHE);
        assertEquals(1, cache1.size(CachePeekMode.ALL));

        for (int i = 0; i < 10; i++) {
            cache1.put(new CustomObject(), UUID.randomUUID().toString());
        }

        forceCheckpoint();
        assertEquals(1, cache1.size(CachePeekMode.ALL));
    }

    private IgniteConfiguration getClientCfg() throws Exception {
        return super.getConfiguration(CLIENT)
            .setClientMode(true);
    }

    private static class CustomObject {
        private int i;

        public CustomObject() {
            i = 1;
        }

        @Override public int hashCode() {
            return 1;
        }

        @Override public boolean equals(Object o) {
            return true;
        }
    }
}
