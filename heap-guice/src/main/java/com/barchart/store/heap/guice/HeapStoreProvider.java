package com.barchart.store.heap.guice;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import rx.Observable;

import com.barchart.store.api.Batch;
import com.barchart.store.api.ObservableIndexQueryBuilder;
import com.barchart.store.api.ObservableQueryBuilder;
import com.barchart.store.api.StoreLockService;
import com.barchart.store.api.StoreService;
import com.barchart.store.api.Table;
import com.barchart.store.heap.HeapLockProvider;
import com.barchart.store.heap.HeapStore;
import com.barchart.util.guice.Activate;
import com.barchart.util.guice.Component;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.typesafe.config.Config;

@Component(HeapStoreProvider.TYPE)
public class HeapStoreProvider implements StoreService, StoreLockService {

	public static final String TYPE = "com.barchart.store.heap";

	@Inject
	@Named("#")
	private Config config;

	private final HeapStore store = new HeapStore();
	private HeapLockProvider locks;

	@Activate
	public void activate() throws Exception {

		if (config.hasPath("lock-db")) {

			final String lockDb = config.getString("lock-db");
			final String lockTable = config.getString("lock-table");
			locks = new HeapLockProvider(store, lockDb, Table.builder(lockTable)
					.rowKey(String.class)
					.columnKey(UUID.class)
					.defaultType(Long.class)
					.build());

		} else {
			locks = null;
		}

	}

	@Override
	public boolean has(final String database) throws Exception {
		return store.has(database);
	}

	@Override
	public void create(final String database) throws Exception {
		store.create(database);
	}

	@Override
	public void delete(final String database) throws Exception {
		store.delete(database);
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> boolean has(final String database,
			final Table<R, C, V> table)
			throws Exception {
		return store.has(database, table);
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> void create(final String database,
			final Table<R, C, V> table)
			throws Exception {
		store.create(database, table);
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> void update(final String database,
			final Table<R, C, V> table)
			throws Exception {
		store.update(database, table);
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> void truncate(final String database,
			final Table<R, C, V> table)
			throws Exception {
		store.truncate(database, table);
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> void delete(final String database,
			final Table<R, C, V> table)
			throws Exception {
		store.delete(database, table);
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> Observable<Boolean> exists(final String database,
			final Table<R, C, V> table, final R keys) throws Exception {
		return store.exists(database, table, keys);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> ObservableQueryBuilder<R, C> fetch(
			final String database,
			final Table<R, C, V> table, final R... keys) throws Exception {
		return store.fetch(database, table, keys);
	}

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> ObservableIndexQueryBuilder<R, C> query(
			final String database, final Table<R, C, V> table) throws Exception {
		return store.query(database, table);
	}

	@Override
	public Batch batch(final String databaseName) throws Exception {
		return store.batch(databaseName);
	}

	@Override
	public Lock localLock(final String name, final long expiration, final TimeUnit units) {
		if (locks == null) {
			throw new UnsupportedOperationException("No lock table has been configured");
		}
		return locks.localLock(name, expiration, units);
	}

	@Override
	public Lock localLock(final String name) {
		if (locks == null) {
			throw new UnsupportedOperationException("No lock table has been configured");
		}
		return locks.localLock(name);
	}

	@Override
	public Lock globalLock(final String name, final long expiration, final TimeUnit units) {
		if (locks == null) {
			throw new UnsupportedOperationException("No lock table has been configured");
		}
		return locks.globalLock(name, expiration, units);
	}

	@Override
	public Lock globalLock(final String name) {
		if (locks == null) {
			throw new UnsupportedOperationException("No lock table has been configured");
		}
		return locks.globalLock(name);
	}

}
