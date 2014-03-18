package com.barchart.store.heap;

import java.util.HashMap;
import java.util.Map;

import rx.Observable;
import rx.Observer;
import rx.Subscription;

import com.barchart.store.api.Batch;
import com.barchart.store.api.ObservableIndexQueryBuilder;
import com.barchart.store.api.ObservableQueryBuilder;
import com.barchart.store.api.StoreRow;
import com.barchart.store.api.StoreService;
import com.barchart.store.api.Table;

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

	/*
	 * @Override public <R extends Comparable<R>> void create(final String
	 * database, final Table<R, String, String> table, final ColumnDef...
	 * columns) throws Exception { getDatabase(database).create(table, columns);
	 * }
	 * 
	 * @Override public <R extends Comparable<R>> void update(final String
	 * database, final Table<R, String, String> table, final ColumnDef...
	 * columns) throws Exception { getDatabase(database).update(table, columns);
	 * }
	 */

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

	@Override
	public <R extends Comparable<R>, C extends Comparable<C>, V> Observable<Boolean> exists(final String database,
			final Table<R, C, V> table, final R keys) throws Exception {

		return Observable.create(new Observable.OnSubscribeFunc<Boolean>() {

			@Override
			public Subscription onSubscribe(
					final Observer<? super Boolean> observer) {

				try {

					@SuppressWarnings("unchecked")
					final Subscription sub =
							fetch(database, table, keys).build().subscribe(
									new Observer<StoreRow<R, C>>() {

										@Override
										public void onCompleted() {
											observer.onCompleted();
										}

										@Override
										public void onError(final Throwable e) {
											observer.onError(e);
										}

										@Override
										public void onNext(final StoreRow<R, C> row) {
											if (row.columns().size() > 0) {
												observer.onNext(true);
											} else {
												observer.onNext(false);
											}
										}

									});

					return new Subscription() {
						@Override
						public void unsubscribe() {
							sub.unsubscribe();
						}
					};

				} catch (final Exception e) {

					observer.onError(e);

					return new Subscription() {
						@Override
						public void unsubscribe() {
						}
					};

				}

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
