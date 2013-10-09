package com.barchart.store.heap;

import java.util.HashMap;
import java.util.Map;

import com.barchart.store.api.Batch;
import com.barchart.store.api.ColumnDef;
import com.barchart.store.api.ObservableQueryBuilder;
import com.barchart.store.api.StoreService;

public class HeapStore implements StoreService {

	private final Map<String, HeapDatabase> databaseMap;

	public HeapStore() {
		this.databaseMap = new HashMap<String, HeapDatabase>();
	}

	@Override
	public boolean has(final String database) throws Exception {
		return databaseMap.containsKey(database);
	}

	@Override
	public void create(final String database) throws Exception {
		if (!databaseMap.containsKey(database)) {
			final HeapDatabase db = new HeapDatabase(database);
			databaseMap.put(database, db);
		}
	}

	@Override
	public void delete(final String database) throws Exception {
		databaseMap.remove(database);
	}

	@Override
	public <K, V> boolean has(final String database, final Table<K, V> table)
			throws Exception {
		return getDatabase(database).has(table);
	}

	@Override
	public <K, V> void create(final String database, final Table<K, V> table)
			throws Exception {
		getDatabase(database).create(table);
	}

	@Override
	public void create(final String database,
			final Table<String, String> table, final ColumnDef... columns)
			throws Exception {
		getDatabase(database).create(table, columns);
	}

	@Override
	public <K, V> void update(final String database, final Table<K, V> table)
			throws Exception {
		getDatabase(database).update(table);
	}

	@Override
	public void update(final String database,
			final Table<String, String> table, final ColumnDef... columns)
			throws Exception {
		getDatabase(database).update(table, columns);
	}

	@Override
	public <K, V> void delete(final String database, final Table<K, V> table)
			throws Exception {
		getDatabase(database).delete(table);
	}

	@Override
	public <K, V> ObservableQueryBuilder<K> fetch(final String database,
			final Table<K, V> table, final String... keys) throws Exception {
		return getDatabase(database).fetch(table, keys);
	}

	@Override
	public <K, V> ObservableQueryBuilder<K> query(final String database,
			final Table<K, V> table, final K column, final Object value)
			throws Exception {
		return getDatabase(database).query(table, column, value);
	}

	@Override
	public Batch batch(final String databaseName) throws Exception {
		return getDatabase(databaseName).batch();
	}

	private HeapDatabase getDatabase(final String databaseName) {
		final HeapDatabase database = databaseMap.get(databaseName);
		if (database == null) {
			throw new IllegalStateException("No database with name: "
					+ databaseName);
		}
		return database;
	}

}
