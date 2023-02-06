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

package org.apache.ignite;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static org.apache.ignite.WalDuringIndexRebuildTest.RebuildType.REMOVE_INDEX_FILE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.configuration.DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;
import static org.apache.ignite.configuration.WALMode.DEFAULT;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.util.IgniteUtils.GB;
import static org.apache.ignite.internal.util.IgniteUtils.MB;

/** */
@RunWith(Parameterized.class)
@SuppressWarnings({"resource", "deprecation"})
public class WalDuringIndexRebuildTest extends GridCommonAbstractTest {
    /** Batch size. */
    public static final int BATCH_SIZE = 10_000;

    /** Batches count. */
    public static final int BATCHES_CNT = 10;

    /** Separator. */
    public static final String SEP = File.separator;

    /** Rebuild type. */
    @Parameter
    public RebuildType rebuildType;

    /** Wal mode. */
    @Parameter(1)
    public WALMode walMode;

    /** */
    @Parameters(name = "rebuildType={0}, walMode={1}")
    public static List<Object[]> parameters() {
        List<Object[]> params = new ArrayList<>();

        for (RebuildType rebuildType : RebuildType.values()) {
            for (WALMode walMode : WALMode.values()) {
                if (walMode != DEFAULT)
                    params.add(new Object[] {rebuildType, walMode});
            }
        }

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setClusterStateOnStart(INACTIVE)
//            .setGridLogger(new NullLogger())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setCheckpointFrequency(10_000)
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(4 * GB)
                    .setPersistenceEnabled(true))
                .setWalMode(walMode)
                .setMaxWalArchiveSize(UNLIMITED_WAL_ARCHIVE));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testIndexRebuild() throws Exception {
        startGrid(0).cluster().state(ACTIVE);

        createAndPopulateCache(grid(0), DEFAULT_CACHE_NAME);

        forceCheckpoint(grid(0));

        WALPointer walPrtBefore = walPtr(grid(0));
        long walIdxBefore = curWalIdx(grid(0));

        String dirName = grid(0).name().replace(".", "_");

        Path idxFileBefore = idxFile(dirName);

        long timestampBefore;

        if (rebuildType == REMOVE_INDEX_FILE) {
            log.warning(">>>>>> Deactivating and removing index file...");

            grid(0).cluster().state(INACTIVE);
            awaitPartitionMapExchange();

            stopGrid(0);

            Files.delete(idxFileBefore);

            startGrid(0);

            log.warning(">>>>>> After restart before activation");

            timestampBefore = System.currentTimeMillis();

            grid(0).cluster().state(ACTIVE);

            log.warning(">>>>>> After activation");
        }
        else {
            timestampBefore = System.currentTimeMillis();

            log.warning(">>>>>> Force index rebuilding...");

            forceRebuildIndexes(grid(0), grid(0).cachex(DEFAULT_CACHE_NAME).context());
        }

        IgniteInternalFuture<?> idxFut = indexRebuildFuture(grid(0), cacheId(DEFAULT_CACHE_NAME));

        idxFut.get();

        long timestampAfter = System.currentTimeMillis();

        log.warning(">>>>>> Index rebuild finished and took " + (timestampAfter - timestampBefore) / 1000 + " seconds");

        forceCheckpoint(grid(0));

        long walIdxAfter = curWalIdx(grid(0));

        log.warning(">>>>>> Index rebuild generated " + (walIdxAfter - walIdxBefore) + " segments");

        Map<RecordType, Long> recTypesBefore = countWalRecordsByTypes(dirName,
            (rt, wp) -> wp.compareTo(walPrtBefore) <= 0);

        Map<RecordType, Long> recTypesAfter = countWalRecordsByTypes(dirName,
            (rt, wp) -> wp.compareTo(walPrtBefore) > 0);

        StringBuilder msgBuilder = new StringBuilder(">>>>>> WalRecords comparison:\n")
            .append(String.format("%-62.60s%-30.28s%-30.28s\n", "Record type", "Data load (before rebuild)",
                "After index rebuild"));

        for (RecordType recType : RecordType.values()) {
            msgBuilder.append(String.format("%-62.60s%-30.28s%-30.28s\n",
                recType,
                recTypesBefore.get(recType),
                recTypesAfter.get(recType)));
        }

        log.warning(msgBuilder.toString());

        idxFile(dirName);
    }

    /**
     * @param ignite Ignite.
     */
    private long curWalIdx(IgniteEx ignite) {
        long curWalIdx = ignite.context()
            .cache()
            .context()
            .wal()
            .currentSegment();

        log.warning(">>>>>> Current WAL segment index: " + curWalIdx);

        return curWalIdx;
    }

    /**
     * @param ignite Ignite.
     */
    private WALPointer walPtr(IgniteEx ignite) {
        WALPointer ptr = ignite.context()
            .cache()
            .context()
            .wal()
            .lastWritePointer();

        log.warning(">>>>>> Last warite WALPointer: " + ptr);

        return ptr;
    }

    /**
     *
     */
    private void createAndPopulateCache(IgniteEx ignite, String cacheName) {
        int fieldsCnt = 50;

        LinkedHashMap<String, String> fields = new LinkedHashMap<>(fieldsCnt);
        List<QueryIndex> indexes = new ArrayList<>(fieldsCnt);


        for (int i = 0; i < fieldsCnt; i++) {
            String fieldName = "F" + i;

            fields.put(fieldName, String.class.getName());

            indexes.add(new QueryIndex(fieldName)
                .setInlineSize(128));
        }

        String testCls = "TestVal";

        QueryEntity qryEntity = new QueryEntity()
            .setKeyType(Integer.class.getName())
            .setValueType(testCls)
            .setFields(fields)
            .setIndexes(indexes);

        IgniteCache<Integer, BinaryObject> cache = ignite.getOrCreateCache(
            new CacheConfiguration<>(cacheName)
                .setQueryEntities(Collections.singleton(qryEntity)))
            .withKeepBinary();

        BinaryObjectBuilder binObjBuilder = ignite.binary().builder(testCls);

        for (String field : fields.keySet())
            binObjBuilder.setField(field, UUID.randomUUID().toString());

        BinaryObject binObj = binObjBuilder.build();

        for (int i = 0; i < BATCHES_CNT; i++) {
            Map<Integer, BinaryObject> vals = new HashMap<>(BATCH_SIZE);

            for (int j = 0; j < BATCH_SIZE; j++)
                vals.put(j + i * BATCH_SIZE, binObj);

            cache.putAll(vals);

            log.warning(">>>>>> Put of " + (i + 1) * BATCH_SIZE + " entries performed.");
        }

        log.warning(">>>>>> Puts finished.");

        assertTrue("Unexpected indexes count", ignite.context().indexProcessor()
            .indexes(cacheName).size() >= fieldsCnt);
    }

    /**
     * @param dirName Directory name.
     */
    private Path idxFile(String dirName) throws IgniteCheckedException, IOException {
        String pathToDfltCacheStr = DFLT_STORE_DIR + SEP + dirName + SEP + "cache-" + DEFAULT_CACHE_NAME;

        File storeDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), pathToDfltCacheStr, false);

        Path idxFile = Files.list(storeDir.toPath())
            .filter(path -> path.endsWith("index.bin"))
            .findFirst()
            .orElseThrow(null);

        assertNotNull("Index file not found", idxFile);

        log.warning(">>>>>> Index file size: " + Files.size(idxFile) / MB + " MB");

        return idxFile;
    }

    /**
     * @param dn2DirName Node directory name.
     * @param predicate Predicate.
     */
    private Map<RecordType, Long> countWalRecordsByTypes(String dn2DirName,
        IgniteBiPredicate<RecordType, WALPointer> predicate) throws IgniteCheckedException {

        File walDir = U.resolveWorkDirectory(U.defaultWorkDirectory(),
            DFLT_STORE_DIR + "/wal/" + dn2DirName, false);

        File walArchiveDir = U.resolveWorkDirectory(U.defaultWorkDirectory(),
            DFLT_STORE_DIR + "/wal/archive/" + dn2DirName, false);

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        IteratorParametersBuilder beforeIdxRemoveBldr = new IteratorParametersBuilder()
            .filesOrDirs(walDir, walArchiveDir)
            .filter(predicate);

        Map<RecordType, Long> cntByRecTypes = new EnumMap<>(RecordType.class);

        for (RecordType recType : RecordType.values())
            cntByRecTypes.put(recType, 0L);

        try (WALIterator walIter = factory.iterator(beforeIdxRemoveBldr)) {
            while (walIter.hasNext()) {
                IgniteBiTuple<WALPointer, WALRecord> entry = walIter.next();

                RecordType recType = entry.getValue().type();

                cntByRecTypes.merge(recType, 1L, Long::sum);
            }
        }

        return cntByRecTypes;
    }

    /**
     *
     */
    public enum RebuildType {
        /** Remove index file. */
        REMOVE_INDEX_FILE,

        /** Force index rebuild. */
        FORCE_INDEX_REBUILD
    }
}
