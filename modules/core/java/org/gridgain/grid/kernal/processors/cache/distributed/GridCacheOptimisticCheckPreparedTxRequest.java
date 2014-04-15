/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.*;

/**
 * Message sent to check that transactions related to some optimistic transaction
 * were prepared on remote node.
 */
public class GridCacheOptimisticCheckPreparedTxRequest<K, V> extends GridDistributedBaseMessage<K, V> {
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private GridUuid futId;

    /** Mini future ID. */
    private GridUuid miniId;

    /** Near transaction ID. */
    private GridCacheVersion nearXidVer;

    /** Expected number of transactions on node. */
    private int txNum;

    /**
     * Empty constructor required by {@link Externalizable}
     */
    public GridCacheOptimisticCheckPreparedTxRequest() {
        // No-op.
    }

    /**
     * @param tx Transaction.
     * @param txNum Expected number of transactions on remote node.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     */
    public GridCacheOptimisticCheckPreparedTxRequest(GridCacheTxEx<K, V> tx, int txNum, GridUuid futId, GridUuid miniId) {
        super(tx.xidVersion(), 0);

        nearXidVer = tx.nearXidVersion();
        this.futId = futId;
        this.miniId = miniId;
        this.txNum = txNum;
    }

    /**
     * @return Near version.
     */
    public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /**
     * @return Future ID.
     */
    public GridUuid futureId() {
        return futId;
    }

    /**
     * @return Mini future ID.
     */
    public GridUuid miniId() {
        return miniId;
    }

    /**
     * @return Expected number of transactions on node.
     */
    public int transactions() {
        return txNum;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridCacheOptimisticCheckPreparedTxRequest _clone = new GridCacheOptimisticCheckPreparedTxRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridCacheOptimisticCheckPreparedTxRequest _clone = (GridCacheOptimisticCheckPreparedTxRequest)_msg;

        _clone.futId = futId;
        _clone.miniId = miniId;
        _clone.nearXidVer = nearXidVer;
        _clone.txNum = txNum;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 7:
                if (!commState.putGridUuid(futId))
                    return false;

                commState.idx++;

            case 8:
                if (!commState.putGridUuid(miniId))
                    return false;

                commState.idx++;

            case 9:
                if (!commState.putCacheVersion(nearXidVer))
                    return false;

                commState.idx++;

            case 10:
                if (!commState.putInt(txNum))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (commState.idx) {
            case 7:
                GridUuid futId0 = commState.getGridUuid();

                if (futId0 == GRID_UUID_NOT_READ)
                    return false;

                futId = futId0;

                commState.idx++;

            case 8:
                GridUuid miniId0 = commState.getGridUuid();

                if (miniId0 == GRID_UUID_NOT_READ)
                    return false;

                miniId = miniId0;

                commState.idx++;

            case 9:
                GridCacheVersion nearXidVer0 = commState.getCacheVersion();

                if (nearXidVer0 == CACHE_VER_NOT_READ)
                    return false;

                nearXidVer = nearXidVer0;

                commState.idx++;

            case 10:
                if (buf.remaining() < 4)
                    return false;

                txNum = commState.getInt();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 18;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheOptimisticCheckPreparedTxRequest.class, this, "super", super.toString());
    }
}
