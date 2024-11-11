package org.apache.ignite.internal.processors.rest.client.message;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteProductVersion;

public class GridClientHandshakeRequestV2 extends GridClientHandshakeRequest {
    /** Ignite version. */
    private IgniteProductVersion ver;

    /**
     * Default constructor.
     */
    public GridClientHandshakeRequestV2() {
        // No-op.
    }

    public GridClientHandshakeRequestV2(IgniteProductVersion ver) {
        this.ver = ver;
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(ver);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        ver = (IgniteProductVersion)in.readObject();
    }

    @Override public String toString() {
        return S.toString(GridClientHandshakeRequestV2.class, this, "igniteVer", ver, "super", super.toString());
    }
}
