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

package org.apache.ignite.internal.client.thin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.DFLT_COMPUTE_TASKS_WARN_THRESHOLD;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.IGNITE_THIN_CLIENT_COMPUTE_TASKS_WARN_INTERVAL_MS;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.IGNITE_THIN_CLIENT_COMPUTE_TASKS_WARN_THRESHOLD;

/**
 * Checks that a server node logs a rate-limited WARN when the active compute tasks count of a thin client connection
 * exceeds the warning threshold (percents of the active compute tasks limit, 80% by default), without changing the
 * client-visible behavior.
 */
public class ClientComputeTasksWarnLogTest extends AbstractThinClientTest {
    /** Active compute tasks limit on the node allowing compute execution. */
    private static final int ACTIVE_TASKS_LIMIT = 3;

    /** Default warning threshold (number of active tasks) with the default 80% at the given limit. */
    private static final int DFLT_WARN_THRESHOLD = ACTIVE_TASKS_LIMIT * DFLT_COMPUTE_TASKS_WARN_THRESHOLD / 100;

    /** Default timeout value. */
    private static final long TIMEOUT = 1_000L;

    /** Timeout to wait until tasks become active on the client side. */
    private static final long ACTIVE_WAIT_TIMEOUT = 10_000L;

    /** Number of limit exceeding attempts for the rate limiting test. */
    private static final int EXCEED_ATTEMPTS = 5;

    /** WARN message prefix logged by the server node. */
    private static final String LIMIT_WARN_MSG = "Active compute tasks per connection warning threshold exceeded";

    /**
     * Listener which matches the exceeded threshold WARN message produced with the default 80% threshold, including
     * connection id, active tasks count, limit and computed threshold, and expects exactly one occurrence per test.
     */
    private static final LogListener LIMIT_WARN_LSNR = LogListener.matches(msg ->
        msg.contains(LIMIT_WARN_MSG)
            && msg.contains("[connId=")
            && msg.contains(", activeTasks=" + ACTIVE_TASKS_LIMIT)
            && msg.contains(", limit=" + ACTIVE_TASKS_LIMIT)
            && msg.contains(", threshold=" + DFLT_WARN_THRESHOLD))
        .times(1)
        .build();

    /**
     * Listener which matches the exceeded threshold WARN message produced with a 50% threshold (2 active tasks,
     * threshold 1), to verify that the threshold is configurable via {@link IgniteSystemProperties}.
     */
    private static final LogListener CONFIGURABLE_WARN_LSNR = LogListener.matches(msg ->
        msg.contains(LIMIT_WARN_MSG)
            && msg.contains("[connId=")
            && msg.contains(", activeTasks=2")
            && msg.contains(", limit=" + ACTIVE_TASKS_LIMIT)
            && msg.contains(", threshold=1"))
        .times(1)
        .build();

    /** Listener which verifies that no limit WARN is logged when compute is disabled. */
    private static final LogListener DISABLED_LIMIT_WARN_LSNR =
        LogListener.matches(LIMIT_WARN_MSG).atMost(0).build();

    /**
     * Listener which verifies that multiple WARN messages are logged when the throttling interval is configured to a
     * small value via {@link IgniteSystemProperties}.
     */
    private static final LogListener INTERVAL_WARN_LSNR = LogListener.matches(LIMIT_WARN_MSG).atLeast(2).build();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ListeningTestLogger srvTestLog = new ListeningTestLogger(log);

        if (getTestIgniteInstanceIndex(igniteInstanceName) == 0) {
            srvTestLog.registerListener(LIMIT_WARN_LSNR);
            srvTestLog.registerListener(CONFIGURABLE_WARN_LSNR);
            srvTestLog.registerListener(INTERVAL_WARN_LSNR);

            cfg.setClientConnectorConfiguration(
                new ClientConnectorConfiguration().setThinClientConfiguration(
                    new ThinClientConfiguration()
                        .setMaxActiveComputeTasksPerConnection(ACTIVE_TASKS_LIMIT)));
        }
        else
            srvTestLog.registerListener(DISABLED_LIMIT_WARN_LSNR);

        cfg.setGridLogger(srvTestLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected boolean isClientEndpointsDiscoveryEnabled() {
        // In this test it's critical to route tasks to a fixed node whose log we listen to.
        return false;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        LatchTask.latch = null;

        LIMIT_WARN_LSNR.reset();
        CONFIGURABLE_WARN_LSNR.reset();
        DISABLED_LIMIT_WARN_LSNR.reset();
        INTERVAL_WARN_LSNR.reset();

        super.afterTest();
    }

    /**
     * Checks that a WARN containing connection id, active tasks count, limit and the computed threshold is logged when
     * the default 80% threshold is exceeded, and that the client still receives the limit exceed error only when the
     * hard limit is reached.
     */
    @Test
    public void testWarnOnDefaultThresholdExceeded() throws Exception {
        try (IgniteClient client = startClient(0)) {
            List<Future<?>> futs = holdTasksActive(client, ACTIVE_TASKS_LIMIT);

            try {
                // The number of active tasks equals the hard limit, which exceeds the default 80% threshold,
                // so the WARN must be present while all started tasks still succeed.
                assertTrue(LIMIT_WARN_LSNR.check(getTestTimeout()));

                // The attempt to exceed the hard limit must not change the client-visible error.
                GridTestUtils.assertThrowsAnyCause(
                    null,
                    () -> client.compute().execute(LatchTask.class.getName(), null),
                    ClientException.class,
                    "limit"
                );
            }
            finally {
                finishTasks(futs);
            }
        }
    }

    /**
     * Checks that repeated limit exceeding attempts produce no more than one WARN per connection
     * within the throttling interval.
     */
    @Test
    public void testWarnRateLimited() throws Exception {
        try (IgniteClient client = startClient(0)) {
            List<Future<?>> futs = holdTasksActive(client, ACTIVE_TASKS_LIMIT);

            try {
                for (int i = 0; i < EXCEED_ATTEMPTS; i++) {
                    GridTestUtils.assertThrowsAnyCause(
                        null,
                        () -> client.compute().execute(LatchTask.class.getName(), null),
                        ClientException.class,
                        "limit"
                    );
                }

                assertTrue(LIMIT_WARN_LSNR.check(getTestTimeout()));
            }
            finally {
                finishTasks(futs);
            }
        }
    }

    /**
     * Checks that no limit WARN is logged when compute is disabled on the node (limit is zero),
     * only the existing disabled functionality error is returned.
     */
    @Test
    public void testNoWarnWhenComputeDisabled() throws Exception {
        try (IgniteClient client = startClient(1)) {
            GridTestUtils.assertThrowsAnyCause(
                null,
                () -> client.compute().execute(LatchTask.class.getName(), null),
                ClientException.class,
                null
            );

            assertTrue(DISABLED_LIMIT_WARN_LSNR.check());
        }
    }

    /**
     * Checks that the warning threshold is configurable via `IGNITE_THIN_CLIENT_COMPUTE_TASKS_WARN_THRESHOLD`:
     * with 50% the WARN is produced as soon as 2 of 3 active/task slots are used (threshold 1), i.e. earlier than with
     * the default 80% (threshold 2).
     */
    @Test
    @WithSystemProperty(key = IGNITE_THIN_CLIENT_COMPUTE_TASKS_WARN_THRESHOLD, value = "50")
    public void testWarnThresholdConfigurable() throws Exception {
        try (IgniteClient client = startClient(0)) {
            List<Future<?>> futs = holdTasksActive(client, 2);

            try {
                assertTrue(CONFIGURABLE_WARN_LSNR.check(getTestTimeout()));
            }
            finally {
                finishTasks(futs);
            }
        }
    }

    /**
     * Checks that the anti-spam interval between WARN messages is configurable via
     * `IGNITE_THIN_CLIENT_COMPUTE_TASKS_WARN_INTERVAL_MS`: with a small interval, repeated limit exceeding attempts
     * produce more than one WARN for the same connection, unlike with the default interval where only one is logged.
     */
    @Test
    @WithSystemProperty(key = IGNITE_THIN_CLIENT_COMPUTE_TASKS_WARN_INTERVAL_MS, value = "1")
    public void testWarnIntervalConfigurable() throws Exception {
        try (IgniteClient client = startClient(0)) {
            List<Future<?>> futs = holdTasksActive(client, ACTIVE_TASKS_LIMIT);

            try {
                for (int i = 0; i < EXCEED_ATTEMPTS; i++) {
                    GridTestUtils.assertThrowsAnyCause(
                        null,
                        () -> client.compute().execute(LatchTask.class.getName(), null),
                        ClientException.class,
                        "limit"
                    );
                }

                assertTrue(INTERVAL_WARN_LSNR.check(getTestTimeout()));
            }
            finally {
                finishTasks(futs);
            }
        }
    }

    /**
     * Starts {@code cnt} latch-gated tasks and waits until they become active on the client side.
     * The returned tasks stay active until {@link #finishTasks(List)} is invoked.
     *
     * @param client Thin client.
     * @param cnt Number of tasks to start.
     * @return Futures of the started tasks.
     */
    private List<Future<?>> holdTasksActive(IgniteClient client, int cnt) throws Exception {
        LatchTask.latch = new CountDownLatch(1);

        List<Future<?>> futs = new ArrayList<>(cnt);

        for (int i = 0; i < cnt; i++)
            futs.add(client.compute().executeAsync(LatchTask.class.getName(), null));

        assertTrue(GridTestUtils.waitForCondition(
            () -> ((ClientComputeImpl)client.compute()).activeTasksCount() == cnt,
            ACTIVE_WAIT_TIMEOUT));

        return futs;
    }

    /**
     * Releases the tasks held by {@link #holdTasksActive(IgniteClient, int)} and waits until they complete.
     *
     * @param futs Futures of the held tasks.
     */
    private void finishTasks(List<Future<?>> futs) throws Exception {
        LatchTask.latch.countDown();

        for (Future<?> fut : futs)
            fut.get(TIMEOUT, TimeUnit.MILLISECONDS);
    }

    /**
     * Test task which is held on a latch at reduction to keep the task active on the server.
     */
    private static class LatchTask extends ComputeTaskAdapter<Long, Void> {
        /** Global latch. */
        private static volatile CountDownLatch latch;

        /** Local latch. */
        private final CountDownLatch locLatch;

        /** Default constructor. */
        private LatchTask() {
            locLatch = latch;
        }

        /** {@inheritDoc} */
        @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(
            List<ClusterNode> subgrid,
            @Nullable Long arg
        ) {
            return subgrid.stream().collect(Collectors.toMap(node -> new TestJob(arg), node -> node));
        }

        /** {@inheritDoc} */
        @Override public @Nullable Void reduce(List<ComputeJobResult> results) throws IgniteException {
            if (locLatch != null) {
                try {
                    locLatch.await(TIMEOUT, TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException ignore) {
                    Thread.currentThread().interrupt();
                }
            }

            return null;
        }
    }
}
