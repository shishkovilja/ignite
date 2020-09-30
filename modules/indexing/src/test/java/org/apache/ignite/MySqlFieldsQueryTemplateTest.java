package org.apache.ignite;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import javax.cache.Cache;
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
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

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

        IgniteCache<PersonKey, MyPerson> cache = grid.cache("SQL_PULIC_PERSON");

        assertNotNull("Cache not found!", cache);

        CacheConfiguration<PersonKey, MyPerson> cacheCfg = cache.getConfiguration(CacheConfiguration.class);

        assertEquals("Backups count is incorect!", 1, cacheCfg.getBackups());
        assertEquals("Atomicity mode is incorrect!", TRANSACTIONAL, cacheCfg.getAtomicityMode());
        assertEquals("Cache mode is incorrect!", CacheMode.PARTITIONED, cacheCfg.getCacheMode());

        PersonKey key = new PersonKey(1, 2);
        MyPerson person = new MyPerson("John Doe", 30, "John Doe & Co");

        cache.put(key, person);

        Iterator<Cache.Entry<PersonKey, MyPerson>> iter = cache.iterator();

        List<Cache.Entry<PersonKey, MyPerson>> entries = new ArrayList<>();

        while (iter.hasNext())
            entries.add(iter.next());

        assertEquals("Only one item should be in cache!", 1, entries.size());

        Cache.Entry<PersonKey, MyPerson> entry = entries.get(0);

        assertNotNull("Entry should not be null!", entry);
        assertEquals("MyPersons should be equal!", person, entry.getValue());
        assertEquals("Person keys should be equal!", key, entry.getKey());

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

        /**
         * @param id Id.
         * @param cityId City id.
         */
        public PersonKey(int id, int cityId) {
            this.id = id;
            this.cityId = cityId;
        }

        /**
         *
         */
        public int getId() {
            return id;
        }

        /**
         * @param id Id.
         */
        public void setId(int id) {
            this.id = id;
        }

        /**
         *
         */
        public int getCityId() {
            return cityId;
        }

        /**
         * @param cityId City id.
         */
        public void setCityId(int cityId) {
            this.cityId = cityId;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            PersonKey key = (PersonKey)o;
            return id == key.id &&
                cityId == key.cityId;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id, cityId);
        }
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

        /**
         * @param name Name.
         * @param age Age.
         * @param company Company.
         */
        public MyPerson(String name, int age, String company) {
            this.name = name;
            this.age = age;
            this.company = company;
        }

        /**
         *
         */
        public String getName() {
            return name;
        }

        /**
         * @param name Name.
         */
        public void setName(String name) {
            this.name = name;
        }

        /**
         *
         */
        public int getAge() {
            return age;
        }

        /**
         * @param age Age.
         */
        public void setAge(int age) {
            this.age = age;
        }

        /**
         *
         */
        public String getCompany() {
            return company;
        }

        /**
         * @param company Company.
         */
        public void setCompany(String company) {
            this.company = company;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            MyPerson person = (MyPerson)o;
            return age == person.age &&
                Objects.equals(name, person.name) &&
                Objects.equals(company, person.company);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(name, age, company);
        }
    }
}

