package com.barchart.store.heap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.barchart.store.api.Batch;
import com.barchart.store.api.ObservableIndexQueryBuilder.Operator;
import com.barchart.store.api.RowMutator;
import com.barchart.store.api.StoreRow;
import com.barchart.store.api.Table;
import com.barchart.util.test.concurrent.TestObserver;

public class TestHeapStore {

	private static final String KEYSPACE = "cs_unit_test";
	private static final Table<String, String, String> TABLE = Table.builder("cs_test_table").build();

	private TestObserver<StoreRow<String, String>> observer;
	private HeapStore store;

	@Test
	public void testKeyspace() throws Exception {
		assertTrue(store.has(KEYSPACE));
		store.delete(KEYSPACE);
		Thread.sleep(100);
		assertFalse(store.has(KEYSPACE));
	}

	@Test
	public void testTable() throws Exception {
		assertFalse(store.has(KEYSPACE, TABLE));
		store.create(KEYSPACE, TABLE);
		Thread.sleep(100);
		assertTrue(store.has(KEYSPACE, TABLE));
		store.delete(KEYSPACE, TABLE);
		Thread.sleep(100);
		assertFalse(store.has(KEYSPACE, TABLE));
	}

	@Test
	public void testQueryMissing() throws Exception {

		store.create(KEYSPACE, TABLE);
		Thread.sleep(100);
		observer.reset();

		store.fetch(KEYSPACE, TABLE, "test-1").build().subscribe(observer);

		assertEquals(1, observer.sync().results.size());
		assertEquals(0, observer.results.get(0).columns().size());

	}

	@Test
	public void testInsert() throws Exception {

		store.create(KEYSPACE, TABLE);
		Thread.sleep(100);

		Batch batch = store.batch(KEYSPACE);
		batch.row(TABLE, "test-1").set("column_key", "column_value");
		batch.commit();

		Thread.sleep(100);

		store.fetch(KEYSPACE, TABLE, "test-1").build().subscribe(observer);

		assertEquals(null, observer.sync().error);
		assertEquals(1, observer.results.size());
		assertEquals(observer.results.get(0).get("column_key").getString(),
				"column_value");

		batch = store.batch(KEYSPACE);
		batch.row(TABLE, "test-2").set("column_key", "column_value2");
		batch.row(TABLE, "test-3").set("column_key", "column_value3");
		batch.commit();

		Thread.sleep(100);

		store.fetch(KEYSPACE, TABLE, "test-2").build()
				.subscribe(observer.reset());

		assertEquals(null, observer.sync().error);
		assertEquals(1, observer.results.size());
		assertEquals(observer.results.get(0).get("column_key").getString(),
				"column_value2");

		store.fetch(KEYSPACE, TABLE, "test-3").build()
				.subscribe(observer.reset());

		assertEquals(null, observer.sync().error);
		assertEquals(1, observer.results.size());
		assertEquals(observer.results.get(0).get("column_key").getString(),
				"column_value3");

	}

	@Test
	public void testIndividualColumns() throws Exception {

		store.create(KEYSPACE, TABLE);
		Thread.sleep(100);

		Batch batch = store.batch(KEYSPACE);
		batch.row(TABLE, "test-1").set("column1", "value1");
		batch.commit();

		batch = store.batch(KEYSPACE);
		batch.row(TABLE, "test-1").set("column2", "value2");
		batch.commit();

		Thread.sleep(100);

		store.fetch(KEYSPACE, TABLE, "test-1").build().subscribe(observer);

		assertEquals(null, observer.sync().error);
		assertEquals(1, observer.results.size());
		assertEquals(observer.results.get(0).get("column1").getString(),
				"value1");
		assertEquals(observer.results.get(0).get("column2").getString(),
				"value2");

	}

	@Test
	public void testColumnLimit() throws Exception {

		store.create(KEYSPACE, TABLE);
		Thread.sleep(100);

		final Batch batch = store.batch(KEYSPACE);
		final RowMutator<String> rm = batch.row(TABLE, "test-1");
		for (int i = 1; i < 100; i++) {
			rm.set("field" + i, "value" + i);
		}
		batch.commit();

		Thread.sleep(100);

		store.fetch(KEYSPACE, TABLE, "test-1").first(1).build()
				.subscribe(observer);

		StoreRow<String, String> row = observer.sync().results.get(0);
		assertEquals(1, row.columns().size());
		assertEquals("field1", row.columns().iterator().next());
		assertEquals("value1", row.get("field1").getString());

		store.fetch(KEYSPACE, TABLE, "test-1").last(1).build()
				.subscribe(observer.reset());

		row = observer.sync().results.get(0);
		assertEquals(1, row.columns().size());
		assertEquals("field99", row.columns().iterator().next());
		assertEquals("value99", row.get("field99").getString());

	}

	@Test
	public void testColumnSlice() throws Exception {

		store.create(KEYSPACE, TABLE);
		Thread.sleep(100);

		final Batch batch = store.batch(KEYSPACE);
		final RowMutator<String> rm = batch.row(TABLE, "test-1");
		for (int i = 1; i < 100; i++) {
			rm.set("field" + i, "value" + i);
		}
		batch.commit();

		Thread.sleep(100);

		store.fetch(KEYSPACE, TABLE, "test-1").start("field10").end("field15")
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

		store.fetch(KEYSPACE, TABLE, "test-1")
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

		store.create(KEYSPACE, TABLE);
		Thread.sleep(100);

		final Batch batch = store.batch(KEYSPACE);
		batch.row(TABLE, "test-1").set("column_key", "column_value1");
		batch.row(TABLE, "test-2").set("column_key", "column_value2");
		batch.row(TABLE, "test-3").set("column_key", "column_value3");
		batch.row(TABLE, "test-4").set("column_key", "column_value4");
		batch.commit();

		Thread.sleep(100);

		store.fetch(KEYSPACE, TABLE).build().subscribe(observer);

		assertEquals(null, observer.sync().error);
		assertEquals(4, observer.results.size());

	}

	@Test
	public void testTruncate() throws Exception {

		store.create(KEYSPACE, TABLE);
		Thread.sleep(100);

		final Batch batch = store.batch(KEYSPACE);
		batch.row(TABLE, "test-1").set("column_key", "column_value1");
		batch.row(TABLE, "test-2").set("column_key", "column_value2");
		batch.row(TABLE, "test-3").set("column_key", "column_value3");
		batch.row(TABLE, "test-4").set("column_key", "column_value4");
		batch.commit();

		Thread.sleep(100);

		store.truncate(KEYSPACE, TABLE);

		store.fetch(KEYSPACE, TABLE).build().subscribe(observer);

		assertEquals(null, observer.sync().error);
		assertEquals(0, observer.results.size());

	}

	@Test
	public void testIndexQuery() throws Exception {

		store.create(KEYSPACE, Table.builder(TABLE.name())
				.rowKey(String.class)
				.columnKey(String.class)
				.defaultType(String.class)
				.column("field", String.class, true)
				.column("num", Integer.class, true)
				.build());

		Thread.sleep(100);

		final Batch batch = store.batch(KEYSPACE);
		for (int i = 1; i < 1000; i++) {
			batch.row(TABLE, "test-" + i).set("field", "value-" + (i % 100))
					.set("num", i);
		}
		batch.commit();

		Thread.sleep(1000);

		store.query(KEYSPACE, TABLE).where("field", "value-50").build()
				.subscribe(observer);

		assertEquals(null, observer.sync().error);
		assertEquals(10, observer.results.size());

		store.query(KEYSPACE, TABLE).where("field", "value-50")
				.where("num", 50).build().subscribe(observer.reset());

		assertEquals(null, observer.sync().error);
		assertEquals(1, observer.results.size());

		store.query(KEYSPACE, TABLE).where("field", "value-50")
				.where("num", 500, Operator.LT).build()
				.subscribe(observer.reset());

		assertEquals(null, observer.sync().error);
		assertEquals(5, observer.results.size());

	}

	@Test
	public void testIndexQuery2() throws Exception {

		final Table<String, Integer, String> table = Table.builder(TABLE.name())
				.rowKey(String.class)
				.columnKey(Integer.class)
				.defaultType(String.class)
				.column(1234, String.class, true)
				.column(1111, Integer.class, true)
				.build();

		store.create(KEYSPACE, table);

		Thread.sleep(100);

		final Batch batch = store.batch(KEYSPACE);
		for (int i = 1; i < 1000; i++) {
			batch.row(table, "test-" + i).set(1234, "value-" + (i % 100))
					.set(1111, i);
		}
		batch.commit();

		Thread.sleep(1000);

		final TestObserver<StoreRow<String, Integer>> obs = TestObserver.create();

		store.query(KEYSPACE, table).where(1234, "value-50").build().subscribe(obs);

		assertEquals(null, obs.sync().error);
		assertEquals(10, obs.results.size());

		store.query(KEYSPACE, table).where(1234, "value-50")
				.where(1111, 50).build().subscribe(obs.reset());

		assertEquals(null, obs.sync().error);
		assertEquals(1, obs.results.size());

		store.query(KEYSPACE, table).where(1234, "value-50")
				.where(1111, 500, Operator.LT).build()
				.subscribe(obs.reset());

		assertEquals(null, obs.sync().error);
		assertEquals(5, obs.results.size());

	}

	@Before
	public void setUp() throws Exception {
		observer = new TestObserver<StoreRow<String, String>>();
		store = new HeapStore();
		try {
			store.delete(KEYSPACE);
		} catch (final Exception e) {
			System.out.println("keyspace missing");
		}
		store.create(KEYSPACE);
	}

	@After
	public void tearDown() throws Exception {
		try {
			store.delete(KEYSPACE);
		} catch (final Exception e) {
			System.out.println("keyspace missing");
		}
		store = null;
	}

}
