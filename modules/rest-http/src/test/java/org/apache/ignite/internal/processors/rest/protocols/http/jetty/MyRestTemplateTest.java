/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.rest.protocols.http.jetty;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 *
 */
public class MyRestTemplateTest extends GridCommonAbstractTest {
    /** Jetty port. */
    private static final int JETTY_PORT = 8080;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        DataRegionConfiguration drCfg = dsCfg.getDefaultDataRegionConfiguration();
        drCfg.setPersistenceEnabled(true);

        cfg.setDataStorageConfiguration(dsCfg);
        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testRest() throws Exception {
        cleanPersistenceDir();

        IgniteEx grid = startGrid();
        grid.cluster().state(ClusterState.ACTIVE);

        final String TEMPLATE = "test";

        grid.addCacheConfiguration(new CacheConfiguration<>(TEMPLATE)
            .setBackups(1)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(TRANSACTIONAL)
        );

        final String CACHE = "myPartionedCache";
        final int TEST_KEY = 1;
        final String TEST_VAL = "testValue";

        final String REQ_PREFIX = "http://localhost:" + JETTY_PORT + "/ignite?cmd=";
        final String CACHE_CREATE = REQ_PREFIX + "getorcreate&cacheName=" + CACHE + "&templateName=test";
        final String PUT_KEY = REQ_PREFIX + "put&key=" + TEST_KEY + "&val=" + TEST_VAL + "&cacheName=" + CACHE +
            "&keyType=int";

        sendRequestAndCheckResponse(CACHE_CREATE, "CACHE_CREATE");

        sendRequestAndCheckResponse(PUT_KEY, "PUT_KEY");

        assertTrue("Cache not found: " + CACHE, grid.cacheNames().contains(CACHE));

        IgniteCache<Integer, String> cache = grid.cache(CACHE);

        CacheConfiguration cacheCfg = cache.getConfiguration(CacheConfiguration.class);

        assertEquals("Backups count is incorect!", 1, cacheCfg.getBackups());
        assertEquals("Atomicity mode is incorrect!", TRANSACTIONAL, cacheCfg.getAtomicityMode());
        assertEquals("Cache mode is incorrect!", CacheMode.PARTITIONED, cacheCfg.getCacheMode());

        String val = cache.get(TEST_KEY);

        assertNotNull("Returned value shouldn't be null!", val);
        assertEquals("Put and got values are not equal!", TEST_VAL, val);

        stopAllGrids();

        cleanPersistenceDir();
    }

    private void sendRequestAndCheckResponse(String CACHE_CREATE, String reqLbl) throws IOException {
        URLConnection conn = new URL(CACHE_CREATE).openConnection();

        conn.connect();

        try (InputStreamReader streamReader = new InputStreamReader(conn.getInputStream())) {
            ObjectMapper objMapper = new ObjectMapper();
            Map<String, Object> myMap = objMapper.readValue(streamReader,
                new TypeReference<Map<String, Object>>() {
                });

            log.info(">>>>>> Perfommed REST operation: requestLabel=" + reqLbl + ", response=" + myMap);

            assertTrue(myMap.containsKey("response"));
            assertEquals(0, myMap.get("successStatus"));
        }
    }
}
