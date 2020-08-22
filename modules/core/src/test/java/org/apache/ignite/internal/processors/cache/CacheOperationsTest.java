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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 *
 */
public class CacheOperationsTest extends GridCommonAbstractTest {
    /**
     * Cache name.
     */
    private static final String cacheName = "cache";

    /**
     * {@inheritDoc}
     */
    @Override protected final IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?, ?> cc = new CacheConfiguration<>(cacheName);

        //cc.setCacheMode(CacheMode.REPLICATED);
        cc.setCacheMode(CacheMode.PARTITIONED);

        //cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC);
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        cc.setAtomicityMode(TRANSACTIONAL);
        //cc.setAtomicityMode(ATOMIC);

        //cc.setNearConfiguration(new NearCacheConfiguration<>());

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected long getTestTimeout() {
        return 300 * 60 * 1000;
    }

    /**
     *
     */
    @Test
    public void test() throws Exception {
        startGrid(0); // Server node
        Ignite client = startClientGrid(1);

        IgniteCache<Integer, Integer> cache = client.getOrCreateCache(cacheName);

        log.warning("PUTS");

        for (int i = 0; i < 200_000; i++)
            cache.put(i, i);

        log.warning("GETS");

        int summ = 0;

        //  for (int j = 0; j < 150; j++) // Enable for Near
        for (int i = 0; i < 200_000; i++)
            summ += cache.get(i);

        log.warning("SUMM=" + summ);
    }
}
