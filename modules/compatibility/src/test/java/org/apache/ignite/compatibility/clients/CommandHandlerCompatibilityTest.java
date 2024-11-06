package org.apache.ignite.compatibility.clients;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.compatibility.testframework.junits.Dependency;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.NotNull;
import org.junit.Assume;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.management.api.CommandUtils.VERSION_MISMATCH_MESSAGE;

/** */
public class CommandHandlerCompatibilityTest extends AbstractClientCompatibilityTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConnectorConfiguration(new ConnectorConfiguration());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        Assume.assumeTrue(VER_2_10_0.compareTo(IgniteProductVersion.fromString(verFormatted)) <= 0);
    }

    /** {@inheritDoc} */
    @Override protected @NotNull Collection<Dependency> getDependencies(String igniteVer) {
        Collection<Dependency> dependencies = super.getDependencies(igniteVer);

        dependencies.add(new Dependency("control-utility", "ignite-control-utility", false));

        return dependencies;
    }

    /** {@inheritDoc} */
    @Override protected void processRemoteConfiguration(IgniteConfiguration cfg) {
        super.processRemoteConfiguration(cfg);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());
    }

    /** {@inheritDoc} */
    @Override protected void testClient(IgniteProductVersion clientVer, IgniteProductVersion serverVer) throws Exception {
        PrintStream out = System.out;
        ByteArrayOutputStream testOut = new ByteArrayOutputStream();
        System.setOut(new PrintStream(testOut));

        boolean verMismatch = !clientVer.equals(serverVer);

        String testOutStr = "";

        try {
            int exitCode = new CommandHandler().execute(List.of("--baseline"));

            testOut.flush();
            testOutStr = testOut.toString();

            int expCode = verMismatch ? EXIT_CODE_UNEXPECTED_ERROR : EXIT_CODE_OK;

            assertEquals(expCode, exitCode);

            if (verMismatch)
                assertTrue(testOutStr.contains(String.format(VERSION_MISMATCH_MESSAGE, serverVer, clientVer)));
        }
        finally {
            System.setOut(out);

            X.print(">>>>>> Test output: " + U.nl() + testOutStr);
        }
    }
}
