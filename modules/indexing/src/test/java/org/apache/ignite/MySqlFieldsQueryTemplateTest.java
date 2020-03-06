package org.apache.ignite;

import java.io.Serializable;
import java.util.List;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

/**
 *
 */
public class MySqlFieldsQueryTemplateTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        DataRegionConfiguration drCfg = dsCfg.getDefaultDataRegionConfiguration();
        drCfg.setPersistenceEnabled(true);

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testSqlFieldsQuery() throws Exception {
        cleanPersistenceDir();

        IgniteEx grid = startGrid();
        grid.cluster().state(ClusterState.ACTIVE);

        final String TEMPLATE = "test";

        grid.addCacheConfiguration(new CacheConfiguration<>(TEMPLATE)
            .setBackups(1)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(ATOMIC)
        );

        String qry = "CREATE TABLE IF NOT EXISTS Person (\n" +
            "  id int,\n" +
            "  city_id int,\n" +
            "  name varchar,\n" +
            "  age int, \n" +
            "  company varchar,\n" +
            "  PRIMARY KEY (id, city_id)\n" +
            ") WITH \"template=test, key_type=PersonKey, value_type=MyPerson\";";

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))) {
            log.info(">>>>>> Caches before SQL: " + client.cacheNames());

            FieldsQueryCursor<List<?>> qryCursor = client.query(new SqlFieldsQuery(qry));

            System.err.println(qryCursor.getAll());

            log.info(">>>>>> Caches after SQL: " + client.cacheNames());
        }

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     *
     */
    private class PersonKey implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 3549411668555128248L;

        /** Id. */
        @QuerySqlField(name = "id")
        private int id;

        /** City id. */
        @QuerySqlField(name = "city_id")
        private int cityId;
    }

    /**
     *
     */
    private class MyPerson implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = -2461620907231474405L;

        /** Name. */
        @QuerySqlField
        private String name;

        /** Age. */
        @QuerySqlField
        private int age;

        /** Company. */
        @QuerySqlField
        private String company;
    }
}

