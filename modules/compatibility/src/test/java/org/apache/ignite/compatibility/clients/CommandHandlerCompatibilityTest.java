package org.apache.ignite.compatibility.clients;

import java.util.List;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.lang.IgniteProductVersion;

/** */
public class CommandHandlerCompatibilityTest extends AbstractClientCompatibilityTest {
    /** {@inheritDoc} */
    @Override protected void testClient(IgniteProductVersion clientVer, IgniteProductVersion serverVer) throws Exception {
        CommandHandler hnd = new CommandHandler();

        hnd.execute(List.of("--baseline"));
    }
}
