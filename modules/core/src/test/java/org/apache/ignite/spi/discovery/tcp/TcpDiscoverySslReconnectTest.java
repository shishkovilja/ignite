package org.apache.ignite.spi.discovery.tcp;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi.DFLT_PORT;

/** */
public class TcpDiscoverySslReconnectTest extends GridCommonAbstractTest {
    /** Unavailable hosts count. */
    private static final int UNAVAILABLE_SERVERS = 100;

    /** Maximum available servers count. */
    private int maxAvailableServers;

    /** Started servers count. */
    private int startedServersCnt;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        startedServersCnt = 0;
        maxAvailableServers = 0;

        super.afterTest();
    }

    /** */
    private IgniteConfiguration getConfiguration(int idx, boolean clientMode, SortedAddressesTcpDiscoverySpi discoSpi)
        throws Exception {
        assertTrue("Max servers count should be greater, then 0", maxAvailableServers > 0);

        if (!clientMode)
            assertTrue("Servers count exceeded 'maxAvailableServers'", startedServersCnt < maxAvailableServers);

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(idx));

        int lastPort = DFLT_PORT + UNAVAILABLE_SERVERS - 1 + maxAvailableServers;

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder()
                .setAddresses(Collections.singleton("127.0.0.1:" + DFLT_PORT + ".." + lastPort)));

        // Client node does not bind to discovery port.
        // Server addresses are being appended to addresses list in direction from the end to start.
        if (!clientMode)
            discoSpi.setLocalPort(lastPort - startedServersCnt);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setSslContextFactory(GridTestUtils.sslFactory());
        cfg.setClientMode(clientMode);

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testTwoServers() throws Exception {
        maxAvailableServers = 2;

        startedServersCnt = 0;

        SortedAddressesTcpDiscoverySpi serverDiscoSpi0 = new SortedAddressesTcpDiscoverySpi();
        startGrid(getConfiguration(0, false, serverDiscoSpi0));

        // Server should try to connect all UNAVAILABLE_HOSTS and maxAvailableServers,
        // excluding 'startedServers' and own address
        assertEquals("Unexpected connection attempts on server0",
            UNAVAILABLE_SERVERS + maxAvailableServers - startedServersCnt - 1,
            serverDiscoSpi0.getConnectionExceptionsCnt());

        startedServersCnt++;

        SortedAddressesTcpDiscoverySpi serverDiscoSpi1 = new SortedAddressesTcpDiscoverySpi();
        startGrid(getConfiguration(1, false, serverDiscoSpi1));

        // The same
        assertEquals("Unexpected connection attempts on server1",
            UNAVAILABLE_SERVERS + maxAvailableServers - startedServersCnt - 1,
            serverDiscoSpi1.getConnectionExceptionsCnt());

        assertEquals("Unexpected servers count", 2, grid(0).cluster().forServers().nodes().size());
    }

    /**
     *
     */
    @Test
    public void testServerAndClient() throws Exception {
        maxAvailableServers = 1;

        SortedAddressesTcpDiscoverySpi serverDiscoSpi = new SortedAddressesTcpDiscoverySpi();
        startGrid(getConfiguration(0, false, serverDiscoSpi));

        // Server should try to connect all UNAVAILABLE_HOSTS and maxAvailableServers,
        // excluding 'startedServers' and own address
        assertEquals("Unexpected connection attempts on server",
            UNAVAILABLE_SERVERS + maxAvailableServers - startedServersCnt - 1,
            serverDiscoSpi.getConnectionExceptionsCnt());

        startedServersCnt++;

        SortedAddressesTcpDiscoverySpi clientDiscoSpi = new SortedAddressesTcpDiscoverySpi();
        startGrid(getConfiguration(1, true, clientDiscoSpi));

        // The same, but client does not exclude its own address, because it does not occupy discovery port.
        // Also take into account perviously started server
        assertEquals("Unexpected connection attempts on client",
            UNAVAILABLE_SERVERS + maxAvailableServers - startedServersCnt,
            clientDiscoSpi.getConnectionExceptionsCnt());

        assertEquals("Unexpected servers count", 1, grid(0).cluster().forServers().nodes().size());
        assertEquals("Unexpected clients count", 1, grid(0).cluster().forClients().nodes().size());
    }

    /**
     *
     */
    @Test
    public void testMultipleServersAndClients() throws Exception {
        maxAvailableServers = 6;

        int startedClientsCnt = 0;

        for (int i = 0; i < maxAvailableServers; i++) {
            SortedAddressesTcpDiscoverySpi serverDiscoSpi = new SortedAddressesTcpDiscoverySpi();
            startGrid(getConfiguration(i, false, serverDiscoSpi));

            // Server should try to connect all UNAVAILABLE_HOSTS and maxAvailableServers,
            // excluding 'startedServers' and own address
            assertEquals("Unexpected connection attempts on ignite#" + i,
                UNAVAILABLE_SERVERS + maxAvailableServers - startedServersCnt - 1,
                serverDiscoSpi.getConnectionExceptionsCnt());

            startedServersCnt++;

            SortedAddressesTcpDiscoverySpi clientDiscoSpi = new SortedAddressesTcpDiscoverySpi();
            startGrid(getConfiguration(i + maxAvailableServers, true, clientDiscoSpi));

            /// The same, but client does not exclude its own address, because it does not occupy discovery port.
            // Also take into account perviously started server
            assertEquals("Unexpected connection attempts on ignite#" + i,
                UNAVAILABLE_SERVERS + maxAvailableServers - startedServersCnt,
                clientDiscoSpi.getConnectionExceptionsCnt());

            startedClientsCnt++;

            assertEquals("Unexpected servers count", startedServersCnt, grid(0).cluster().forServers().nodes().size());
            assertEquals("Unexpected clients count", startedClientsCnt, grid(0).cluster().forClients().nodes().size());
        }

        assertEquals("Unexpected servers count", maxAvailableServers, grid(0).cluster().forServers().nodes().size());
        assertEquals("Unexpected clients count", maxAvailableServers, grid(0).cluster().forClients().nodes().size());
    }

    /**
     * TcpDiscoverySpi which ensures sorted collection of addresses.
     */
    private static class SortedAddressesTcpDiscoverySpi extends TcpDiscoverySpi {
        /** Connection exceptions count. */
        private int connectionExceptionsCnt;

        /** {@inheritDoc} */
        @Override protected Collection<InetSocketAddress> resolvedAddresses() throws IgniteSpiException {
            Collection<InetSocketAddress> addresses0 = super.resolvedAddresses();

            Comparator<InetSocketAddress> byPortComparator = Comparator.comparingInt(InetSocketAddress::getPort);

            // ClientImpl iterates over addresses collection with reverse order.
            if (isClientMode())
                byPortComparator = byPortComparator.reversed();

            Set<InetSocketAddress> addresses = new TreeSet<>(byPortComparator);
            addresses.addAll(addresses0);

            log.warning(">>>>>> Sorted addresses collection: [size = " + addresses.size() + ", addresses=" +
                addresses + ']');

            return addresses;
        }

        /** {@inheritDoc} */
        @Override protected Socket openSocket(InetSocketAddress sockAddr,
            IgniteSpiOperationTimeoutHelper timeoutHelper) throws IOException, IgniteSpiOperationTimeoutException {
            try {
                return super.openSocket(sockAddr, timeoutHelper);
            }
            // Count exceptions caused by hosts unavailability and re-throw these exceptions
            catch (ConnectException e) {
                connectionExceptionsCnt++;

                throw e;
            }
        }

        /** */
        public int getConnectionExceptionsCnt() {
            return connectionExceptionsCnt;
        }
    }
}
