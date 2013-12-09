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
import org.junit.Test;

import com.barchart.store.api.Batch;
import com.barchart.store.api.ColumnDef;
import com.barchart.store.api.ObservableIndexQueryBuilder.Operator;
import com.barchart.store.api.RowMutator;
import com.barchart.store.api.StoreRow;
import com.barchart.store.api.StoreService.Table;
import com.barchart.util.test.concurrent.CallableTest;
import com.barchart.util.test.concurrent.TestObserver;
import com.netflix.astyanax.util.TimeUUIDUtils;

// Tests should be reliable now on Jenkins
//@Ignore
public class TestCassandraStore {

	private static String keyspace;
	private static CassandraStore store;

	private Table<String, String> table;
	private TestObserver<StoreRow<String>> observer;

	@Test
	public void testExistence() throws Exception {

		final Batch batch = store.batch(keyspace);
		batch.row(table, "test-1").set("column_key", "column_value");
		batch.commit();

		Thread.sleep(100);

		final TestObserver<Boolean> existsObserver =
				new TestObserver<Boolean>();

		store.exists(keyspace, table, "test-1").subscribe(existsObserver);

		assertTrue(existsObserver.sync().results.get(0));

		store.exists(keyspace, table, "test-2").subscribe(
				existsObserver.reset());

		assertFalse(existsObserver.sync().results.get(0));

	}

	@Test
	public void testQueryMissing() throws Exception {

		store.fetch(keyspace, table, "test-1").build().subscribe(observer);

		assertEquals(1, observer.sync().results.size());
		assertEquals(0, observer.results.get(0).columns().size());

	}

	@Test
	public void testInsert() throws Exception {

		Batch batch = store.batch(keyspace);
		batch.row(table, "test-1").set("column_key", "column_value");
		batch.commit();

		Thread.sleep(100);

		store.fetch(keyspace, table, "test-1").build().subscribe(observer);

		assertEquals(null, observer.sync().error);
		assertEquals(1, observer.results.size());
		assertEquals(observer.results.get(0).get("column_key").getString(),
				"column_value");

		batch = store.batch(keyspace);
		batch.row(table, "test-2").set("column_key", "column_value2");
		batch.row(table, "test-3").set("column_key", "column_value3");
		batch.commit();

		Thread.sleep(100);

		store.fetch(keyspace, table, "test-2").build()
				.subscribe(observer.reset());

		assertEquals(null, observer.sync().error);
		assertEquals(1, observer.results.size());
		assertEquals(observer.results.get(0).get("column_key").getString(),
				"column_value2");

		store.fetch(keyspace, table, "test-3").build()
				.subscribe(observer.sync().reset());

		assertEquals(null, observer.sync().error);
		assertEquals(1, observer.results.size());
		assertEquals(observer.results.get(0).get("column_key").getString(),
				"column_value3");

	}

	@Test
	public void testUUID() throws Exception {

		final Table<UUID, String> uuidTable =
				Table.make(randomName(), UUID.class, String.class);
		store.create(keyspace, uuidTable);

		CallableTest.waitFor(new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				return store.has(keyspace, uuidTable);
			}
		}, 5000);

		final UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

		final Batch batch = store.batch(keyspace);
		batch.row(uuidTable, "test-1").set(uuid, "column_value");
		batch.commit();

		Thread.sleep(100);

		final TestObserver<StoreRow<UUID>> uuidObserver =
				new TestObserver<StoreRow<UUID>>();

		store.fetch(keyspace, uuidTable, "test-1").build()
				.subscribe(uuidObserver);

		assertEquals(null, uuidObserver.sync().error);
		assertEquals(1, uuidObserver.results.size());
		assertEquals(uuidObserver.results.get(0).get(uuid).getString(),
				"column_value");

	}

	@Test
	public void testColumnLimit() throws Exception {

		final Batch batch = store.batch(keyspace);
		final RowMutator<String> rm = batch.row(table, "test-1");
		for (int i = 1; i < 100; i++) {
			rm.set("field" + i, "value" + i);
		}
		batch.commit();

		Thread.sleep(100);

		store.fetch(keyspace, table, "test-1").first(1).build()
				.subscribe(observer);

		StoreRow<String> row = observer.sync().results.get(0);
		assertEquals(1, row.columns().size());
		assertEquals("field1", row.columns().iterator().next());
		assertEquals("value1", row.get("field1").getString());

		store.fetch(keyspace, table, "test-1").last(1).build()
				.subscribe(observer.reset());

		row = observer.sync().results.get(0);
		assertEquals(1, row.columns().size());
		assertEquals("field99", row.columns().iterator().next());
		assertEquals("value99", row.get("field99").getString());

	}

	@Test
	public void testColumnSlice() throws Exception {

		final Batch batch = store.batch(keyspace);
		final RowMutator<String> rm = batch.row(table, "test-1");
		for (int i = 1; i < 100; i++) {
			rm.set("field" + i, "value" + i);
		}
		batch.commit();

		Thread.sleep(100);

		store.fetch(keyspace, table, "test-1").start("field10").end("field15")
				.build().subscribe(observer);

		StoreRow<String> row = observer.sync().results.get(0);
		assertEquals(6, row.columns().size());

		// Test indexed sort order
		assertEquals("field10", row.getByIndex(0).getName());
		assertEquals("field11", row.getByIndex(1).getName());
		assertEquals("field12", row.getByIndex(2).getName());
		assertEquals("field13", row.getByIndex(3).getName());
		assertEquals("field14", row.getByIndex(4).getName());
		assertEquals("field15", row.getByIndex(5).getName());

		store.fetch(keyspace, table, "test-1")
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

		final Batch batch = store.batch(keyspace);
		batch.row(table, "test-1").set("column_key", "column_value1");
		batch.row(table, "test-2").set("column_key", "column_value2");
		batch.row(table, "test-3").set("column_key", "column_value3");
		batch.row(table, "test-4").set("column_key", "column_value4");
		batch.commit();

		Thread.sleep(100);

		store.fetch(keyspace, table).build().subscribe(observer);

		assertEquals(null, observer.sync().error);
		assertEquals(4, observer.results.size());

	}

	@Test
	public void testIndexQuery() throws Exception {

		final Table<String, String> indexTable = Table.make(randomName());

		store.create(keyspace, indexTable, new ColumnDef() {

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

		Thread.sleep(100);

		final Batch batch = store.batch(keyspace);
		for (int i = 1; i < 1000; i++) {
			batch.row(indexTable, "test-" + i)
					.set("field", "value-" + (i % 100)).set("num", i);
		}
		batch.commit();

		Thread.sleep(1000);

		store.query(keyspace, indexTable).where("field", "value-50").build()
				.subscribe(observer);

		assertEquals(null, observer.sync().error);
		assertEquals(10, observer.results.size());

		store.query(keyspace, indexTable).where("field", "value-50")
				.where("num", 50).build().subscribe(observer.reset());

		assertEquals(null, observer.sync().error);
		assertEquals(1, observer.results.size());

		store.query(keyspace, indexTable).where("field", "value-50")
				.where("num", 500, Operator.LT).build()
				.subscribe(observer.reset());

		assertEquals(null, observer.sync().error);
		assertEquals(5, observer.results.size());

	}

	@Before
	public void prepare() throws Exception {
		table = Table.make(randomName());
		observer = new TestObserver<StoreRow<String>>();
		store.create(keyspace, table);
		try {
			CallableTest.waitFor(new Callable<Boolean>() {
				@Override
				public Boolean call() throws Exception {
					return store.has(keyspace, table);
				}
			}, 5000);
		} catch (final Exception e) {
			store.delete(keyspace, table);
			throw e;
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

		keyspace = randomName();
		store.create(keyspace);
		try {
			CallableTest.waitFor(new Callable<Boolean>() {
				@Override
				public Boolean call() throws Exception {
					return store.has(keyspace);
				}
			}, 5000);
		} catch (final Exception e) {
			store.delete(keyspace);
			throw e;
		}
	}

	// tearDown() will kill all tables during keyspace removal
	// @After
	public void cleanUp() throws Exception {
		try {
			store.delete(keyspace, table);
		} catch (final Exception e) {
			System.out.println(e.getMessage());
		}
	}

	@AfterClass
	public static void tearDown() throws Exception {
		if (store != null) {
			if (keyspace != null) {
				try {
					store.delete(keyspace);
				} catch (final Exception e) {
					System.out.println(e.getMessage());
				}
			}
			store.disconnect();
			store = null;
		}
	}

	/** NOTE: must be under 48 chars in size. */
	private static String randomName() {
		return TestCassandraStore.class.getSimpleName() + "_"
				+ System.nanoTime();
	}

}
