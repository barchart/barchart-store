package com.barchart.store.heap;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import rx.Observable;
import rx.functions.Func1;

import com.barchart.store.api.Batch;
import com.barchart.store.api.ObservableIndexQueryBuilder;
import com.barchart.store.api.ObservableQueryBuilder;
import com.barchart.store.api.StoreRow;
import com.barchart.store.api.StoreService;
import com.barchart.store.api.Table;
import com.barchart.store.util.TimeUUIDComparator;

public class HeapStore implements StoreService {

	private final Map<String, HeapDatabase> databaseMap;

	static final Comparator<UUID> UUID_COMPARATOR = new TimeUUIDComparator();

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
	public <R extends Comparable<R>, C extends Comparable<C>, V> boolean has(final String database, final Table<R, C, V> table)
			throws Exception {
		return getDatabase(database).has(table);
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> void create(final String database,
			final Table<R, C, V> table)
			throws Exception {
		getDatabase(database).create(table);
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> void update(final String database,
			final Table<R, C, V> table)
			throws Exception {
		getDatabase(database).update(table);
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> void truncate(final String database,
			final Table<R, C, V> table)
			throws Exception {
		getDatabase(database).truncate(table);
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> void delete(final String database, final Table<R, C, V> table)
			throws Exception {
		getDatabase(database).delete(table);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> Observable<Boolean> exists(final String database,
			final Table<R, C, V> table, final R key) throws Exception {

		return fetch(database, table, key).build().exists(new Func1<StoreRow<R, C>, Boolean>() {

			@Override
			public Boolean call(final StoreRow<R, C> row) {
				return row.columns().size() > 0;
			}

		});

	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> ObservableQueryBuilder<R, C> fetch(final String database,
			final Table<R, C, V> table, final R... keys) throws Exception {
		return getDatabase(database).fetch(table, keys);
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> ObservableIndexQueryBuilder<R, C> query(final String database,
			final Table<R, C, V> table) throws Exception {
		return getDatabase(database).query(table);
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
