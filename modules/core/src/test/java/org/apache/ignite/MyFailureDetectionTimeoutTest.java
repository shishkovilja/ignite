package org.apache.ignite;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test failure detection timeout 'spam'.
 */
public class MyFailureDetectionTimeoutTest extends GridCommonAbstractTest {
    /** Block lstnr. */
    private LogListener blockLstnr = LogListener
        .matches("Blocked system-critical thread has been detected.")
        .build();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        ListeningTestLogger listeningLog = new ListeningTestLogger();

        listeningLog.registerListener(blockLstnr);

        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(listeningLog);
    }

    /**
     * Checks log for messages about system critial thread blocking for different failure timeout values
     */
    @Test
    public void testBlockingMessages() throws Exception {
        final int INIT_TIMOUT = 2000;
        final int TIMEOUT_STEP = 2000;
        final int MAX_TIMEOUT = 20001;

        List<Integer> timeoutsWithFoundBlockedMsgs = new ArrayList<>();

        int failureTimeout = INIT_TIMOUT;

        while (failureTimeout < MAX_TIMEOUT) {
            assertFalse(blockLstnr.check());

            log.info(">>>>>> Testing timeout: " + failureTimeout);

            IgniteEx grid = startGrid(getConfiguration().setFailureDetectionTimeout(failureTimeout));

            boolean blocked = GridTestUtils.waitForCondition(() -> blockLstnr.check(), failureTimeout * 2);

            if (blocked) {
                log.warning(">>>>>> Blocked system critical thread DETECTED");
                timeoutsWithFoundBlockedMsgs.add(failureTimeout);
            }

            stopGrid(grid.configuration().getIgniteInstanceName());

            blockLstnr.reset();

            failureTimeout += TIMEOUT_STEP;
        }

        if (!timeoutsWithFoundBlockedMsgs.isEmpty()) {
            log.warning(
                "Blocked system critical threads DETECTED for these 'failure detection timeout' values:\n\t" +
                    timeoutsWithFoundBlockedMsgs);
        }

        assertTrue("Blocked system critical thread messages found!", timeoutsWithFoundBlockedMsgs.isEmpty());
    }
}
