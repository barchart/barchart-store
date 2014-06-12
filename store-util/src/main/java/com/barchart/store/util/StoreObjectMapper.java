package com.barchart.store.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import rx.Observable;
import rx.Observer;
import rx.functions.Func1;

import com.barchart.store.api.Batch;
import com.barchart.store.api.ObservableIndexQueryBuilder;
import com.barchart.store.api.ObservableIndexQueryBuilder.Operator;
import com.barchart.store.api.ObservableQueryBuilder;
import com.barchart.store.api.RowMutator;
import com.barchart.store.api.StoreService;
import com.barchart.store.api.Table;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class StoreObjectMapper {

	static final ObjectMapper mapper = new ObjectMapper();
	static {
		mapper.setVisibilityChecker(mapper.getSerializationConfig()
				.getDefaultVisibilityChecker()
				.withFieldVisibility(JsonAutoDetect.Visibility.ANY)
				.withGetterVisibility(JsonAutoDetect.Visibility.NONE)
				.withSetterVisibility(JsonAutoDetect.Visibility.NONE)
				.withCreatorVisibility(JsonAutoDetect.Visibility.NONE));
	}

	protected final StoreService store;
	protected final String database;
	protected final StoreSchema schema;

	private int maxReadBatch = 100;
	private int maxWriteBatch = 100;

	private final MapperFactory mappers;

	public StoreObjectMapper(final StoreService store_, final String database_,
			final StoreSchema schema_) {

		store = store_;
		database = database_;
		schema = schema_;

		mappers = new MapperFactory();

	}

	/**
	 * Set the maximum batch size for row read queries to prevent unbounded
	 * multi-gets.
	 */
	public void maxReadBatch(final int size) {
		maxReadBatch = size;
	}

	/**
	 * The maximum batch size for row read queries. Default 100.
	 */
	public int maxReadBatch() {
		return maxReadBatch;
	}

	/**
	 * Set the maximum batch size for row updates to limit memory consumption.
	 */
	public void maxWriteBatch(final int size) {
		maxWriteBatch = size;
	}

	/**
	 * The maximum batch size for row updates. Default 100.
	 */
	public int maxWriteBatch() {
		return maxWriteBatch;
	}

	/**
	 * Update the data store schema definition. This should only be called
	 * manually.
	 */
	public void updateSchema() throws Exception {
		schema.update(store, database);
	}

	/*
	 * Direct store access methods for subclasses with more complex query
	 * requirements.
	 */

	protected StoreService store() {
		return store;
	}

	protected String database() {
		return database;
	}

	protected <M> M mapper(final Class<M> cls) {
		return mappers.instance(cls);
	}

	/*
	 * Helper methods for subclasses.
	 */

	/**
	 * Load rows from a store table and convert them to objects.
	 *
	 * @param table The store table
	 * @param mapper The object mapper
	 * @param keys The row keys to load
	 * @return A lazy observable that executes on every subscribe
	 */
	protected <R extends Comparable<R>, C extends Comparable<C>, V, T, M extends RowMapper<R, C, T>> Observable<T> loadRows(
			final Table<R, C, V> table, final Class<M> mapper,
			final R... keys) {

		return batch(new Func1<R[], Observable<T>>() {

			@Override
			public Observable<T> call(final R[] slice) {

				try {
					return store.fetch(database, table, slice).build().lift(mapper(mapper));
				} catch (final Exception e) {
					return Observable.error(e);
				}

			}

		}, keys, maxReadBatch);

	}

	/**
	 * Load columns from a table row and convert them to objects.
	 *
	 * @param table The store table
	 * @param mapper The object mapper
	 * @param key The row key to load
	 * @param columns The column names to load
	 * @return A lazy observable that executes on every subscribe
	 */
	protected <R extends Comparable<R>, C extends Comparable<C>, V, T, M extends ColumnListMapper<R, C, T>> Observable<T> loadColumns(
			final Table<R, C, V> table, final Class<M> mapper, final R key, final C... columns) {

		try {

			@SuppressWarnings("unchecked")
			final ObservableQueryBuilder<R, C> query = store.fetch(database, table, key);

			if (columns != null && columns.length > 0) {
				query.columns(columns);
			}

			return query.build().lift(mapper(mapper));

		} catch (final Exception e) {
			return Observable.error(e);
		}

	}

	/**
	 * Load columns from a table row and convert them to objects.
	 *
	 * @param table The store table
	 * @param mapper The object mapper
	 * @param key The row key to load
	 * @param prefix The column name prefix to filter by
	 * @return A lazy observable that executes on every subscribe
	 */
	@SuppressWarnings("unchecked")
	protected <R extends Comparable<R>, V, T, M extends ColumnListMapper<R, String, T>> Observable<T> loadColumnsByPrefix(
			final Table<R, String, V> table, final Class<M> mapper, final R key, final String prefix) {

		try {
			return store.fetch(database, table, key).prefix(prefix).build().lift(mapper(mapper));
		} catch (final Exception e) {
			return Observable.error(e);
		}

	}

	/**
	 * Load columns from a table row and convert them to objects.
	 *
	 * @param table The store table
	 * @param mapper The object mapper
	 * @param key The row key to load
	 * @param count The number of columns to load in sorted order
	 * @param reverse Sort descending instead of ascending
	 * @return A lazy observable that executes on every subscribe
	 */
	protected <R extends Comparable<R>, C extends Comparable<C>, V, T, M extends ColumnListMapper<R, C, T>> Observable<T> loadColumns(
			final Table<R, C, V> table, final Class<M> mapper, final R key, final int count, final boolean reverse) {

		try {

			return store.fetch(database, table, key)
					.reverse(reverse)
					.limit(count)
					.build()
					.lift(mapper(mapper));

		} catch (final Exception e) {
			return Observable.error(e);
		}

	}

	/**
	 *
	 * Load a range of columns from a table row and convert them to objects.
	 *
	 * @param table The store table
	 * @param mapper The object mapper
	 * @param key The row key to load
	 * @param start The column name to start the range with
	 * @param end The column name to end the range with
	 * @return A lazy observable that executes on every subscribe
	 */
	@SuppressWarnings("unchecked")
	protected <R extends Comparable<R>, C extends Comparable<C>, V, T, M extends ColumnListMapper<R, C, T>> Observable<T> loadColumns(
			final Table<R, C, V> table, final Class<M> mapper, final R key, final C start, final C end) {

		try {
			return store.fetch(database, table, key).start(start).end(end).build().lift(mapper(mapper));
		} catch (final Exception e) {
			return Observable.error(e);
		}

	}

	/**
	 * Find rows based on secondary index values.
	 *
	 * @param table The store table
	 * @param mapper The object mapper
	 * @param clauses Clauses for index searching
	 * @return A lazy observable that executes on every subscribe
	 */
	@SafeVarargs
	protected final <R extends Comparable<R>, C extends Comparable<C>, V, T, M extends RowMapper<R, C, T>> Observable<T> findRows(
			final Table<R, C, V> table, final Class<M> mapper, final Where<C>... clauses) {

		try {

			final ObservableIndexQueryBuilder<R, C> builder =
					store.query(database, table);

			for (final Where<C> where : clauses) {
				builder.where(where.field, where.value, where.operator);
			}

			return builder.build().lift(mapper(mapper));

		} catch (final Exception e) {
			return Observable.error(e);
		}

	}

	/**
	 * Create a new row in the store from the specified object.
	 *
	 * @param table The store table
	 * @param mapper The object mapper
	 * @param key The row key
	 * @param obj The object to store
	 * @return A cached observable that is executed immediately
	 */
	@SuppressWarnings("unchecked")
	protected <R extends Comparable<R>, C extends Comparable<C>, V, T, U extends T, M extends RowMapper<R, C, T>> Observable<T> createRow(
			final Table<R, C, V> table, final Class<M> mapper, final R key, final U obj) {

		return cache(loadRows(table, mapper, key).isEmpty().flatMap(
				new Func1<Boolean, Observable<T>>() {

					@Override
					public Observable<T> call(final Boolean empty) {
						if (empty) {
							return updateRow(table, mapper, key, obj);
						} else {
							return Observable.error(new Exception(
									"Object ID already exists"));
						}
					}

				}));

	}

	/**
	 * Update a row in the store with the specified object.
	 *
	 * @param table The store table
	 * @param mapper The object mapper
	 * @param key The row key
	 * @param obj The object to store
	 * @return A cached observable that is executed immediately
	 */
	protected <R extends Comparable<R>, C extends Comparable<C>, V, T, U extends T, M extends RowMapper<R, C, T>> Observable<T> updateRow(
			final Table<R, C, V> table, final Class<M> mapper, final R key, final U obj) {

		try {

			final Batch batch = store.batch(database);
			final RowMutator<C> mutator = batch.row(table, key);

			mapper(mapper).encode(obj, mutator);
			batch.commit();

			return Observable.<T> from(obj);

		} catch (final Exception e) {
			return Observable.error(e);
		}

	}

	/**
	 * Load columns from a table row and convert them to objects.
	 *
	 * @param table The store table
	 * @param mapper The object mapper
	 * @param key The row key
	 * @param objects The objects to store as columns
	 * @return A cached observable that is executed immediately
	 */
	protected <R extends Comparable<R>, C extends Comparable<C>, V, T, U extends T, M extends ColumnListMapper<R, C, T>> Observable<T> updateColumns(
			final Table<R, C, V> table, final Class<M> mapper, final R key, final U... objects) {

		try {

			final Batch batch = store.batch(database);
			final RowMutator<C> mutator = batch.row(table, key);
			final M m = mapper(mapper);
			for (final U obj : objects) {
				m.encode(obj, mutator);
			}
			batch.commit();

			return Observable.<T> from(objects);

		} catch (final Exception e) {
			return Observable.error(e);
		}

	}

	/**
	 * Delete rows from the store by key.
	 *
	 * @param table The store table
	 * @param keys The row keys to delete
	 * @return A cached observable that is executed immediately
	 */
	protected <R extends Comparable<R>, C extends Comparable<C>, V> Observable<R> deleteRows(
			final Table<R, C, V> table, final R... keys) {

		return batch(new Func1<R[], Observable<R>>() {

			@Override
			public Observable<R> call(final R[] slice) {

				try {

					final Batch batch = store.batch(database);
					for (final R key : slice) {
						batch.row(table, key).delete();
					}
					batch.commit();

					return Observable.from(slice);

				} catch (final Exception e) {
					return Observable.error(e);
				}

			}

		}, keys, maxWriteBatch);

	}

	/**
	 * Delete columns from a row in the store.
	 *
	 * @param table The store table
	 * @param key The row key
	 * @param columns The column names to delete
	 * @return A cached observable that is executed immediately
	 */
	protected <R extends Comparable<R>, C extends Comparable<C>, V> Observable<C> deleteColumns(
			final Table<R, C, V> table, final R key, final C... columns) {

		try {

			final Batch batch = store.batch(database);
			final RowMutator<C> row = batch.row(table, key);

			for (final C column : columns) {
				row.remove(column);
			}

			batch.commit();

			return Observable.from(columns);

		} catch (final Exception e) {
			return Observable.error(e);
		}

	}

	protected static String[] toStrings(final Object[] ary) {

		final String[] strings = new String[ary.length];

		for (int i = 0; i < ary.length; i++) {
			strings[i] = ary[i].toString();
		}

		return strings;

	}

	/**
	 * Auto subscribe to an observable, caching the result for future
	 * subscriptions.
	 */
	protected static <T> Observable<T> cache(final Observable<T> observable) {

		final Observable<T> cached = observable.cache();

		cached.subscribe(new Observer<T>() {

			@Override
			public void onCompleted() {
			}

			@Override
			public void onError(final Throwable e) {
			}

			@Override
			public void onNext(final T args) {
			}

		});

		return cached;

	}

	/**
	 * Split a set of objects into batches and run a task over each batch. This
	 * should be used extensively when requesting objects over multiple rows,
	 * since unbounded multi-get queries can kill a cluster very quickly.
	 *
	 * http://www.datastax.com/documentation/cassandra/1.2/cassandra/
	 * architecture/architecturePlanningAntiPatterns_c.html?scroll=
	 * concept_ds_emm_hwl_fk__multiple-gets
	 */
	protected static <T, K> Observable<T> batch(final Func1<K[], Observable<T>> task, final K[] params,
			final int batchSize) {

		if (params.length <= batchSize) {
			return task.call(params);
		}

		final List<Observable<T>> results = new ArrayList<Observable<T>>();

		for (final K[] slice : slice(params, batchSize)) {
			results.add(task.call(slice));
		}

		return Observable.mergeDelayError(Observable.from(results));

	}

	/**
	 * Slice a key set into multiple batches.
	 */
	protected static <T> List<T[]> slice(final T[] keys, final int batchSize) {

		final ArrayList<T[]> batches = new ArrayList<T[]>();

		if (batchSize == 0 || keys.length <= batchSize) {
			batches.add(keys);
		} else {

			int idx = 0;

			while (idx <= keys.length) {
				final int end = idx + batchSize > keys.length ? keys.length : idx + batchSize;
				batches.add(Arrays.copyOfRange(keys, idx, end));
				idx += batchSize;
			}

		}

		return batches;

	}

	protected static String[] toStrings(final List<?> list) {

		final String[] strings = new String[list.size()];

		for (int i = 0; i < list.size(); i++) {
			strings[i] = list.get(i).toString();
		}

		return strings;

	}

	protected static class Where<T> {

		private final T field;
		private final Object value;
		private final Operator operator;

		public Where(final T field_, final Object value_,
				final Operator operator_) {
			field = field_;
			value = value_;
			operator = operator_;
		}

		public static <T> Where<T> clause(final T field, final Object value) {
			return new Where<T>(field, value, Operator.EQUAL);
		}

		public static <T> Where<T> clause(final T field, final Object value,
				final Operator operator) {
			return new Where<T>(field, value, operator);
		}

	}

}
