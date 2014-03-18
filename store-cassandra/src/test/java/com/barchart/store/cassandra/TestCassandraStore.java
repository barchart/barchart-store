package com.barchart.store.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.barchart.store.api.Batch;
import com.barchart.store.api.ColumnDef;
import com.barchart.store.api.ObservableIndexQueryBuilder.Operator;
import com.barchart.store.api.RowMutator;
import com.barchart.store.api.StoreRow;
import com.barchart.store.api.Table;
import com.barchart.util.test.concurrent.CallableTest;
import com.barchart.util.test.concurrent.TestObserver;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.util.TimeUUIDUtils;

// Low entropy, run manually on functional updates
@Ignore
public class TestCassandraStore {

	private static final String KEYSPACE = "store_unit_test";

	private static final Table<String, String, String> DEFAULT_TABLE = Table.make("default");
	private static final Table<String, UUID, String> UUID_COL_TABLE =
			Table.make("uuid", String.class, UUID.class, String.class);
	private static final Table<Integer, String, String> INT_ROW_TABLE =
			Table.make("introw", Integer.class, String.class, String.class);
	private static final Table<String, String, String> INDEX_TABLE = Table.make("index");

	private static final Table<?, ?, ?>[] TABLES = new Table<?, ?, ?>[] {
			DEFAULT_TABLE, UUID_COL_TABLE, INT_ROW_TABLE, INDEX_TABLE
	};

	private static CassandraStore store;

	private TestObserver<StoreRow<String, String>> observer;

	@Test
	public void testExistence() throws Exception {

		final Batch batch = store.batch(KEYSPACE);
		batch.row(DEFAULT_TABLE, "test-1").set("column_key", "column_value");
		batch.commit();

		Thread.sleep(100);

		final TestObserver<Boolean> existsObserver =
				new TestObserver<Boolean>();

		store.exists(KEYSPACE, DEFAULT_TABLE, "test-1").subscribe(existsObserver);

		assertTrue(existsObserver.sync().results.get(0));

		store.exists(KEYSPACE, DEFAULT_TABLE, "test-2").subscribe(
				existsObserver.reset());

		assertFalse(existsObserver.sync().results.get(0));

	}

	@Test
	public void testQueryMissing() throws Exception {

		store.fetch(KEYSPACE, DEFAULT_TABLE, "test-1").build().subscribe(observer);

		assertEquals(1, observer.sync().results.size());
		assertEquals(0, observer.results.get(0).columns().size());

	}

	@Test
	public void testInsert() throws Exception {

		Batch batch = store.batch(KEYSPACE);
		batch.row(DEFAULT_TABLE, "test-1").set("column_key", "column_value");
		batch.commit();

		Thread.sleep(100);

		store.fetch(KEYSPACE, DEFAULT_TABLE, "test-1").build().subscribe(observer);

		assertEquals(null, observer.sync().error);
		assertEquals(1, observer.results.size());
		assertEquals(observer.results.get(0).get("column_key").getString(),
				"column_value");

		batch = store.batch(KEYSPACE);
		batch.row(DEFAULT_TABLE, "test-2").set("column_key", "column_value2");
		batch.row(DEFAULT_TABLE, "test-3").set("column_key", "column_value3");
		batch.commit();

		Thread.sleep(100);

		store.fetch(KEYSPACE, DEFAULT_TABLE, "test-2").build()
				.subscribe(observer.reset());

		assertEquals(null, observer.sync().error);
		assertEquals(1, observer.results.size());
		assertEquals(observer.results.get(0).get("column_key").getString(),
				"column_value2");

		store.fetch(KEYSPACE, DEFAULT_TABLE, "test-3").build()
				.subscribe(observer.sync().reset());

		assertEquals(null, observer.sync().error);
		assertEquals(1, observer.results.size());
		assertEquals(observer.results.get(0).get("column_key").getString(),
				"column_value3");

	}

	@Test
	public void testUUID() throws Exception {

		final UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

		final Batch batch = store.batch(KEYSPACE);
		batch.row(UUID_COL_TABLE, "test-1").set(uuid, "column_value");
		batch.commit();

		Thread.sleep(100);

		final TestObserver<StoreRow<String, UUID>> uuidObserver = new TestObserver<StoreRow<String, UUID>>();

		store.fetch(KEYSPACE, UUID_COL_TABLE, "test-1").build().subscribe(uuidObserver);

		assertEquals(null, uuidObserver.sync().error);
		assertEquals(1, uuidObserver.results.size());
		assertEquals(uuidObserver.results.get(0).get(uuid).getString(), "column_value");

	}

	@Test
	public void testIntRow() throws Exception {

		final Batch batch = store.batch(KEYSPACE);
		batch.row(INT_ROW_TABLE, 1234).set("key", "value");
		batch.commit();

		Thread.sleep(100);

		final TestObserver<StoreRow<Integer, String>> obs = new TestObserver<StoreRow<Integer, String>>();

		store.fetch(KEYSPACE, INT_ROW_TABLE, 1234).build().subscribe(obs);

		assertEquals(null, obs.sync().error);
		assertEquals(1, obs.results.size());
		assertEquals(1234, (int) obs.results.get(0).getKey());
		assertEquals(obs.results.get(0).get("key").getString(), "value");

		store.fetch(KEYSPACE, INT_ROW_TABLE, 1111).build().subscribe(obs.reset());

		assertEquals(null, obs.sync().error);
		assertEquals(1, obs.results.size());
		assertEquals(0, obs.results.get(0).columns().size());

	}

	@Test
	public void testColumnLimit() throws Exception {

		final Batch batch = store.batch(KEYSPACE);
		final RowMutator<String> rm = batch.row(DEFAULT_TABLE, "test-1");
		for (int i = 1; i < 100; i++) {
			rm.set("field" + i, "value" + i);
		}
		batch.commit();

		Thread.sleep(100);

		store.fetch(KEYSPACE, DEFAULT_TABLE, "test-1").first(1).build()
				.subscribe(observer);

		StoreRow<String, String> row = observer.sync().results.get(0);
		assertEquals(1, row.columns().size());
		assertEquals("field1", row.columns().iterator().next());
		assertEquals("value1", row.get("field1").getString());

		store.fetch(KEYSPACE, DEFAULT_TABLE, "test-1").last(1).build()
				.subscribe(observer.reset());

		row = observer.sync().results.get(0);
		assertEquals(1, row.columns().size());
		assertEquals("field99", row.columns().iterator().next());
		assertEquals("value99", row.get("field99").getString());

	}

	@Test
	public void testColumnSlice() throws Exception {

		final Batch batch = store.batch(KEYSPACE);
		final RowMutator<String> rm = batch.row(DEFAULT_TABLE, "test-1");
		for (int i = 1; i < 100; i++) {
			rm.set("field" + i, "value" + i);
		}
		batch.commit();

		Thread.sleep(100);

		store.fetch(KEYSPACE, DEFAULT_TABLE, "test-1").start("field10").end("field15")
				.build().subscribe(observer);

		StoreRow<String, String> row = observer.sync().results.get(0);
		assertEquals(6, row.columns().size());

		// Test indexed sort order
		assertEquals("field10", row.getByIndex(0).getName());
		assertEquals("field11", row.getByIndex(1).getName());
		assertEquals("field12", row.getByIndex(2).getName());
		assertEquals("field13", row.getByIndex(3).getName());
		assertEquals("field14", row.getByIndex(4).getName());
		assertEquals("field15", row.getByIndex(5).getName());

		store.fetch(KEYSPACE, DEFAULT_TABLE, "test-1")
				.columns("field10", "field15", "field20", "field1000").build()
				.subscribe(observer.reset());

		row = observer.sync().results.get(0);
		// field1000 doesn't exist, so 3 instead of 4
		assertEquals(3, row.columns().size());

		// Test indexed sort order
		assertEquals("field10", row.getByIndex(0).getName());
		assertEquals("field15", row.getByIndex(1).getName());
		assertEquals("field20", row.getByIndex(2).getName());

	}

	@Test
	public void testQueryAll() throws Exception {

		final Batch batch = store.batch(KEYSPACE);
		batch.row(DEFAULT_TABLE, "test-1").set("column_key", "column_value1");
		batch.row(DEFAULT_TABLE, "test-2").set("column_key", "column_value2");
		batch.row(DEFAULT_TABLE, "test-3").set("column_key", "column_value3");
		batch.row(DEFAULT_TABLE, "test-4").set("column_key", "column_value4");
		batch.commit();

		Thread.sleep(100);

		store.fetch(KEYSPACE, DEFAULT_TABLE).build().subscribe(observer);

		assertEquals(null, observer.sync().error);
		assertEquals(4, observer.results.size());

	}

	@Test
	public void testTruncate() throws Exception {

		final Batch batch = store.batch(KEYSPACE);
		batch.row(DEFAULT_TABLE, "test-1").set("column_key", "column_value1");
		batch.row(DEFAULT_TABLE, "test-2").set("column_key", "column_value2");
		batch.row(DEFAULT_TABLE, "test-3").set("column_key", "column_value3");
		batch.row(DEFAULT_TABLE, "test-4").set("column_key", "column_value4");
		batch.commit();

		Thread.sleep(100);

		store.truncate(KEYSPACE, DEFAULT_TABLE);

		Thread.sleep(100);

		store.fetch(KEYSPACE, DEFAULT_TABLE).build().subscribe(observer);

		assertEquals(null, observer.sync().error);
		assertEquals(0, observer.results.size());

	}

	@Test
	public void testIndexQuery() throws Exception {

		final Batch batch = store.batch(KEYSPACE);
		for (int i = 1; i < 1000; i++) {
			batch.row(INDEX_TABLE, "test-" + i)
					.set("field", "value-" + (i % 100)).set("num", i);
		}
		batch.commit();

		Thread.sleep(1000);

		store.query(KEYSPACE, INDEX_TABLE).where("field", "value-50").build()
				.subscribe(observer);

		assertEquals(null, observer.sync().error);
		assertEquals(10, observer.results.size());

		store.query(KEYSPACE, INDEX_TABLE).where("field", "value-50")
				.where("num", 50).build().subscribe(observer.reset());

		assertEquals(null, observer.sync().error);
		assertEquals(1, observer.results.size());

		store.query(KEYSPACE, INDEX_TABLE).where("field", "value-50")
				.where("num", 500, Operator.LT).build()
				.subscribe(observer.reset());

		assertEquals(null, observer.sync().error);
		assertEquals(5, observer.results.size());

	}

	@Before
	public void prepare() throws Exception {
		observer = new TestObserver<StoreRow<String, String>>();
		for (final Table<?, ?, ?> table : TABLES) {
			store.truncate(KEYSPACE, table);
		}
	}

	@BeforeClass
	public static void setUp() throws Exception {

		final Properties props = new Properties();
		props.load(TestCassandraStore.class
				.getResourceAsStream("/cassandra.store"));

		final String[] seeds = props.get("seeds").toString().split(",");
		final String username = props.get("username").toString();
		final String password = props.get("password").toString();

		store = new CassandraStore();
		store.setSeeds(seeds);
		store.setCredentials(username, password);
		store.connect();
		store.setReadConsistency(ConsistencyLevel.CL_ALL);
		store.setWriteConsistency(ConsistencyLevel.CL_ALL);

		if (!store.has(KEYSPACE)) {
			store.create(KEYSPACE);
			CallableTest.waitFor(new Callable<Boolean>() {
				@Override
				public Boolean call() throws Exception {
					return store.has(KEYSPACE);
				}
			}, 5000);
		}

		// Special def
		if (!store.has(KEYSPACE, INDEX_TABLE)) {
			store.create(KEYSPACE, INDEX_TABLE, new ColumnDef() {

				@Override
				public String key() {
					return "field";
				}

				@Override
				public boolean isIndexed() {
					return true;
				}

				@Override
				public Class<?> type() {
					return String.class;
				}

			}, new ColumnDef() {

				@Override
				public String key() {
					return "num";
				}

				@Override
				public boolean isIndexed() {
					return true;
				}

				@Override
				public Class<?> type() {
					return Integer.class;
				}

			});
			CallableTest.waitFor(new Callable<Boolean>() {
				@Override
				public Boolean call() throws Exception {
					return store.has(KEYSPACE, INDEX_TABLE);
				}
			}, 5000);
		}

		for (final Table<?, ?, ?> table : TABLES) {
			if (!store.has(KEYSPACE, table)) {
				store.create(KEYSPACE, table);
				CallableTest.waitFor(new Callable<Boolean>() {
					@Override
					public Boolean call() throws Exception {
						return store.has(KEYSPACE, table);
					}
				}, 5000);
			}
		}

	}

	@AfterClass
	public static void tearDown() throws Exception {
		if (store != null) {
			store.disconnect();
			store = null;
		}
	}

}
