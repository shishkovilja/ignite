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

package org.apache.ignite.internal;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterGroupEmptyException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.events.JobEvent;
import org.apache.ignite.events.TaskEvent;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVTS_ALL_MINUS_METRIC_UPDATE;
import static org.apache.ignite.events.EventType.EVTS_JOB_EXECUTION;
import static org.apache.ignite.events.EventType.EVTS_TASK_EXECUTION;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_TASK_STARTED;
import static org.apache.ignite.internal.util.lang.ClusterNodeFunc.remoteNodes;

/**
 * Event storage tests.
 *
 * Note:
 * Test based on events generated by test task execution.
 * Filter class must be static because it will be send to remote host in
 * serialized form.
 */
@GridCommonTest(group = "Kernal Self")
public class GridEventStorageSelfTest extends GridCommonAbstractTest {
    /** First grid. */
    private static Ignite ignite1;

    /** Second grid. */
    private static Ignite ignite2;

    /** */
    public GridEventStorageSelfTest() {
        super(/*start grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setIncludeEventTypes(EventType.EVTS_ALL);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite1 = startGrid(1);
        ignite2 = startGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        ignite1 = null;
        ignite2 = null;
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testAddRemoveGlobalListener() throws Exception {
        IgnitePredicate<Event> lsnr = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                info("Received local event: " + evt);

                return true;
            }
        };

        ignite1.events().localListen(lsnr, EVTS_ALL_MINUS_METRIC_UPDATE);

        assert ignite1.events().stopLocalListen(lsnr);
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testAddRemoveDiscoListener() throws Exception {
        IgnitePredicate<Event> lsnr = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                info("Received local event: " + evt);

                return true;
            }
        };

        ignite1.events().localListen(lsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);

        assert ignite1.events().stopLocalListen(lsnr);
        assert !ignite1.events().stopLocalListen(lsnr);
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testLocalNodeEventStorage() throws Exception {
        TestEventListener lsnr = new TestEventListener();

        IgnitePredicate<Event> filter = new TestEventFilter();

        // Check that two same listeners may be added.
        ignite1.events().localListen(lsnr, EVT_TASK_STARTED);
        ignite1.events().localListen(lsnr, EVT_TASK_STARTED);

        // Execute task.
        generateEvents(ignite1);

        assert lsnr.getCounter() == 1;

        Collection<Event> evts = ignite1.events().localQuery(filter);

        assert evts != null;
        assert evts.size() == 1;

        // Execute task.
        generateEvents(ignite1);

        // Check that listener has been removed.
        assert lsnr.getCounter() == 2;

        // Check that no problems with nonexistent listeners.
        assert ignite1.events().stopLocalListen(lsnr);
        assert !ignite1.events().stopLocalListen(lsnr);

        // Check for events from local node.
        evts = ignite1.events().localQuery(filter);

        assert evts != null;
        assert evts.size() == 2;

        // Check for events from empty remote nodes collection.
        try {
            events(ignite1.cluster().forPredicate(F.<ClusterNode>alwaysFalse())).remoteQuery(filter, 0);
        }
        catch (ClusterGroupEmptyException ignored) {
            // No-op
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testRemoteNodeEventStorage() throws Exception {
        IgnitePredicate<Event> filter = new TestEventFilter();

        generateEvents(ignite2);

        ClusterGroup prj = ignite1.cluster().forPredicate(remoteNodes(ignite1.cluster().localNode().id()));

        Collection<Event> evts = events(prj).remoteQuery(filter, 0);

        assert evts != null;
        assert evts.size() == 1;
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testRemoteAndLocalNodeEventStorage() throws Exception {
        IgnitePredicate<Event> filter = new TestEventFilter();

        generateEvents(ignite1);

        ClusterGroup prj = ignite1.cluster().forPredicate(remoteNodes(ignite1.cluster().localNode().id()));

        Collection<Event> evts = ignite1.events().remoteQuery(filter, 0);
        Collection<Event> locEvts = ignite1.events().localQuery(filter);
        Collection<Event> remEvts = events(prj).remoteQuery(filter, 0);

        assert evts != null;
        assert locEvts != null;
        assert remEvts != null;
        assert evts.size() == 1;
        assert locEvts.size() == 1;
        assert remEvts.isEmpty();
    }

    /**
     * Checks that specified event is not task or job event.
     *
     * @param evt Event to check.
     */
    private void checkGridInternalEvent(Event evt) {
        assertFalse("Found TASK event for task marked with @GridInternal [evtType=" + evt.type() + "]", evt instanceof TaskEvent);
        assertFalse("Found JOB event for task marked with @GridInternal [evtType=" + evt.type() + "]", evt instanceof JobEvent);
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testGridInternalEvents() throws Exception {
        IgnitePredicate<Event> lsnr = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                checkGridInternalEvent(evt);

                return true;
            }
        };

        ignite1.events().localListen(lsnr, EVTS_TASK_EXECUTION);
        ignite1.events().localListen(lsnr, EVTS_JOB_EXECUTION);
        ignite2.events().localListen(lsnr, EVTS_TASK_EXECUTION);
        ignite2.events().localListen(lsnr, EVTS_JOB_EXECUTION);

        executeGridInternalTask(ignite1);

        Collection<Event> evts1 = ignite1.events().localQuery(F.<Event>alwaysTrue());
        Collection<Event> evts2 = ignite2.events().localQuery(F.<Event>alwaysTrue());

        assert evts1 != null;
        assert evts2 != null;

        for (Event evt : evts1)
            checkGridInternalEvent(evt);

        for (Event evt : evts2)
            checkGridInternalEvent(evt);

        assert ignite1.events().stopLocalListen(lsnr, EVTS_TASK_EXECUTION);
        assert ignite1.events().stopLocalListen(lsnr, EVTS_JOB_EXECUTION);
        assert ignite2.events().stopLocalListen(lsnr, EVTS_TASK_EXECUTION);
        assert ignite2.events().stopLocalListen(lsnr, EVTS_JOB_EXECUTION);
    }

    /**
     * Create events in grid.
     *
     * @param ignite Grid.
     */
    private void generateEvents(Ignite ignite) {
        ignite.compute().localDeployTask(GridEventTestTask.class, GridEventTestTask.class.getClassLoader());

        ignite.compute().execute(GridEventTestTask.class.getName(), null);
    }

    /**
     * Execute task marged with {@code GridInternal} annotation.
     *
     * @param ignite Grid.
     */
    private void executeGridInternalTask(Ignite ignite) {
        ignite.compute().execute(GridInternalTestTask.class.getName(), null);
    }

    /**
     * Test task.
     */
    private static class GridEventTestTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            return Collections.singleton(new GridEventTestJob());
        }

        /** {@inheritDoc} */
        @Override public Serializable reduce(List<ComputeJobResult> results) {
            assert results != null;
            assert results.size() == 1;

            return results.get(0).getData();
        }
    }

    /**
     * Test job.
     */
    private static class GridEventTestJob extends ComputeJobAdapter {
        /** {@inheritDoc} */
        @Override public String execute() {
            return "GridEventTestJob-test-event.";
        }
    }

    /**
     * Test task marked with @GridInternal.
     */
    @GridInternal
    private static class GridInternalTestTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            Collection<ComputeJob> jobs = new ArrayList<>(gridSize);

            for (int i = 0; i < gridSize; i++)
                jobs.add(new GridInternalTestJob());

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Serializable reduce(List<ComputeJobResult> results) {
            assert results != null;

            return "GridInternalTestTask-result.";
        }
    }

    /**
     * Test job.
     */
    private static class GridInternalTestJob extends ComputeJobAdapter {
        /** {@inheritDoc} */
        @Override public String execute() {
            return "GridInternalTestJob-result.";
        }
    }

    /**
     * Test event listener.
     */
    private class TestEventListener implements IgnitePredicate<Event> {
        /** Event counter. */
        private AtomicInteger cnt = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            info("Event storage event: evt=" + evt);

            // Count only started tasks.
            if (evt.type() == EVT_TASK_STARTED)
                cnt.incrementAndGet();

            return true;
        }

        /**
         * @return Event counter value.
         */
        public int getCounter() {
            return cnt.get();
        }

        /**
         * Clear event counter.
         */
        public void clearCounter() {
            cnt.set(0);
        }
    }

    /**
     * Test event filter.
     */
    private static class TestEventFilter implements IgnitePredicate<Event> {
        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            // Accept only predefined TASK_STARTED events.
            return evt.type() == EVT_TASK_STARTED;
        }
    }
}
