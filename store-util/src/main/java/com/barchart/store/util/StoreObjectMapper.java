package com.barchart.store.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import rx.Observable;
import rx.Observer;
import rx.Subscriber;
import rx.functions.Func0;
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
	private final Map<Table<?, ?, ?>, TableCache<?, ?, ?>> caches = new HashMap<Table<?, ?, ?>, TableCache<?, ?, ?>>();

	public StoreObjectMapper(final StoreService store_, final String database_,
			final StoreSchema schema_) {

		store = store_;
		database = database_;
		schema = schema_;

		mappers = new MapperFactory();

	}

	/**
	 * Set the maximum batch size for row read queries to prevent unbounded multi-gets.
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
	 * Update the data store schema definition. This should only be called manually.
	 */
	public void updateSchema() throws Exception {
		schema.update(store, database);
	}

	/**
	 * Set the cache for a table.
	 */
	public <R extends Comparable<R>, C extends Comparable<C>, V> void setTableCache(final Table<R, C, V> table,
			final TableCache<R, C, ?> cache) {
		caches.put(table, cache);
	}

	/*
	 * Direct store access methods for subclasses with more complex query requirements.
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
	@SuppressWarnings("unchecked")
	protected <R extends Comparable<R>, C extends Comparable<C>, V, T, M extends RowMapper<R, C, T>> Observable<T> loadRows(
			final Table<R, C, V> table, final Class<M> mapper, final R... keys) {

		final TableCache<R, C, T> cache = (TableCache<R, C, T>) caches.get(table);

		final BatchLoader<R, T> loader =
				new BatchLoader<R, T>(new RowLoader<R, C, V, T, M>(table, mapper), maxReadBatch);

		if (cache == null)
			return loader.call(keys);

		return cache.rows(loader, keys);

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
	@SuppressWarnings("unchecked")
	protected <R extends Comparable<R>, C extends Comparable<C>, V, T, M extends ColumnListMapper<R, C, T>> Observable<T> loadColumns(
			final Table<R, C, V> table, final Class<M> mapper, final R key, final C... columns) {

		final TableCache<R, C, T> cache = (TableCache<R, C, T>) caches.get(table);

		final ColumnKeyLoader<R, C, V, T, M> loader = new ColumnKeyLoader<R, C, V, T, M>(table, mapper, key);

		if (cache == null)
			return loader.call(columns);

		return cache.columns(loader, key, columns);

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

		final TableCache<R, String, T> cache = (TableCache<R, String, T>) caches.get(table);

		final ColumnPrefixLoader<R, String, V, T, M> loader =
				new ColumnPrefixLoader<R, String, V, T, M>(table, mapper, key, prefix);

		if (cache == null)
			return loader.call();

		return cache.columnsByPrefix(loader, key, prefix);

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
	@SuppressWarnings("unchecked")
	protected <R extends Comparable<R>, C extends Comparable<C>, V, T, M extends ColumnListMapper<R, C, T>> Observable<T> loadColumns(
			final Table<R, C, V> table, final Class<M> mapper, final R key, final int count, final boolean reverse) {

		final TableCache<R, C, T> cache = (TableCache<R, C, T>) caches.get(table);

		final ColumnSliceLoader<R, C, V, T, M> loader =
				new ColumnSliceLoader<R, C, V, T, M>(table, mapper, key, count, reverse);

		if (cache == null)
			return loader.call();

		return cache.columns(loader, key, count, reverse);

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

		final TableCache<R, C, T> cache = (TableCache<R, C, T>) caches.get(table);

		final ColumnRangeLoader<R, C, V, T, M> loader =
				new ColumnRangeLoader<R, C, V, T, M>(table, mapper, key, start, end);

		if (cache == null)
			return loader.call();

		return cache.columns(loader, key, start, end);

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

							final TableCache<R, C, T> cache = (TableCache<R, C, T>) caches.get(table);
							if (cache != null) {
								cache.update(key, obj);
							}

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
	@SuppressWarnings("unchecked")
	protected <R extends Comparable<R>, C extends Comparable<C>, V, T, U extends T, M extends RowMapper<R, C, T>> Observable<T> updateRow(
			final Table<R, C, V> table, final Class<M> mapper, final R key, final U obj) {

		try {

			final TableCache<R, C, T> cache = (TableCache<R, C, T>) caches.get(table);
			if (cache != null) {
				cache.update(key, obj);
			}

			final Batch batch = store.batch(database);
			final RowMutator<C> mutator = batch.row(table, key);

			mapper(mapper).encode(obj, mutator);

			return batch.commit().lift(new SuccessResult<T>(obj));

		} catch (final Exception e) {
			return Observable.error(e);
		}

	}

	/**
	 * Update columns in a table row with the specific objects.
	 *
	 * @param table The store table
	 * @param mapper The object mapper
	 * @param key The row key
	 * @param objects The objects to store as columns
	 * @return A cached observable that is executed immediately
	 */
	@SuppressWarnings("unchecked")
	protected <R extends Comparable<R>, C extends Comparable<C>, V, T, U extends T, M extends ColumnListMapper<R, C, T>> Observable<T> updateColumns(
			final Table<R, C, V> table, final Class<M> mapper, final R key, final U... objects) {

		try {

			final TableCache<R, C, T> cache = (TableCache<R, C, T>) caches.get(table);

			final Batch batch = store.batch(database);
			final RowMutator<C> mutator = batch.row(table, key);
			final M m = mapper(mapper);
			for (final U obj : objects) {

				final C col = m.encode(obj, mutator);

				if (cache != null && col != null) {
					cache.update(key, col, obj);
				}

			}

			return batch.commit().lift(new SuccessResult<T>(objects));

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
	@SuppressWarnings("unchecked")
	protected <R extends Comparable<R>, C extends Comparable<C>, V> Observable<R> deleteRows(
			final Table<R, C, V> table, final R... keys) {

		final TableCache<R, C, ?> cache = (TableCache<R, C, ?>) caches.get(table);

		return new BatchLoader<R, R>(new Func1<R[], Observable<R>>() {

			@Override
			public Observable<R> call(final R[] slice) {

				try {

					final Batch batch = store.batch(database);
					for (final R key : slice) {

						if (cache != null) {
							cache.invalidate(key);
						}

						batch.row(table, key).delete();
					}
					return batch.commit().lift(new SuccessResult<R>(slice));

				} catch (final Exception e) {
					return Observable.error(e);
				}

			}

		}, maxWriteBatch).call(keys);

	}

	/**
	 * Delete columns from a row in the store.
	 *
	 * @param table The store table
	 * @param key The row key
	 * @param columns The column names to delete
	 * @return A cached observable that is executed immediately
	 */
	@SuppressWarnings("unchecked")
	protected <R extends Comparable<R>, C extends Comparable<C>, V> Observable<C> deleteColumns(
			final Table<R, C, V> table, final R key, final C... columns) {

		final TableCache<R, C, ?> cache = (TableCache<R, C, ?>) caches.get(table);

		try {

			final Batch batch = store.batch(database);
			final RowMutator<C> row = batch.row(table, key);

			for (final C column : columns) {

				if (cache != null) {
					cache.invalidate(key, column);
				}

				row.remove(column);

			}

			return batch.commit().lift(new SuccessResult<C>(columns));

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
	 * Auto subscribe to an observable, caching the result for future subscriptions.
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
			public void onNext(final T t) {
			}

		});

		return cached;

	}

	/**
	 * Split a set of objects into batches and run a task over each batch. This should be used extensively when
	 * requesting objects over multiple rows, since unbounded multi-get queries can kill a cluster very quickly.
	 *
	 * http://www.datastax.com/documentation/cassandra/1.2/cassandra/
	 * architecture/architecturePlanningAntiPatterns_c.html?scroll= concept_ds_emm_hwl_fk__multiple-gets
	 */
	protected static <T, K> Observable<T> batch(final Func1<K[], Observable<T>> task,
			final K[] params, final int batchSize) {
		return new BatchLoader<K, T>(task, batchSize).call(params);
	}

	protected static String[] toStrings(final List<?> list) {

		final String[] strings = new String[list.size()];

		for (int i = 0; i < list.size(); i++) {
			strings[i] = list.get(i).toString();
		}

		return strings;

	}

	protected static class SuccessResult<T> implements Observable.Operator<T, Boolean> {

		private final T[] result;

		@SuppressWarnings("unchecked")
		protected SuccessResult(final T... result_) {
			result = result_;
		}

		@Override
		public Subscriber<? super Boolean> call(final Subscriber<? super T> subscriber) {

			return new Subscriber<Boolean>(subscriber) {

				@Override
				public void onCompleted() {
					for (final T item : result)
						subscriber.onNext(item);
					subscriber.onCompleted();
				}

				@Override
				public void onError(final Throwable e) {
					subscriber.onError(e);
				}

				@Override
				public void onNext(final Boolean success) {
				}

			};

		}

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

	/**
	 * Split a set of objects into batches and run a task over each batch. This should be used extensively when
	 * requesting objects over multiple rows, since unbounded multi-get queries can kill a cluster very quickly.
	 *
	 * http://www.datastax.com/documentation/cassandra/1.2/cassandra/
	 * architecture/architecturePlanningAntiPatterns_c.html?scroll= concept_ds_emm_hwl_fk__multiple-gets
	 */
	protected static class BatchLoader<K, T> implements Func1<K[], Observable<T>> {

		private final Func1<K[], Observable<T>> task;
		private final int batchSize;

		public BatchLoader(final Func1<K[], Observable<T>> task_, final int batchSize_) {
			task = task_;
			batchSize = batchSize_;
		}

		@Override
		public Observable<T> call(final K[] keys) {

			if (keys.length <= batchSize) {
				return task.call(keys);
			}

			final List<Observable<T>> results = new ArrayList<Observable<T>>();

			for (final K[] slice : slice(keys, batchSize)) {
				results.add(task.call(slice));
			}

			return Observable.mergeDelayError(Observable.from(results));

		}

		/**
		 * Slice a key set into multiple batches.
		 */
		private static <T> List<T[]> slice(final T[] keys, final int batchSize) {

			final ArrayList<T[]> batches = new ArrayList<T[]>();

			if (batchSize == 0 || keys.length <= batchSize) {
				batches.add(keys);
			} else {

				int idx = 0;

				while (idx < keys.length) {
					final int end = idx + batchSize > keys.length ? keys.length : idx + batchSize;
					batches.add(Arrays.copyOfRange(keys, idx, end));
					idx += batchSize;
				}

			}

			return batches;

		}

	}

	protected class RowLoader<R extends Comparable<R>, C extends Comparable<C>, V, T, M extends RowMapper<R, C, T>>
			implements Func1<R[], Observable<T>> {

		private final Table<R, C, V> table;
		private final Class<M> mapper;

		public RowLoader(final Table<R, C, V> table_, final Class<M> mapper_) {
			table = table_;
			mapper = mapper_;
		}

		@Override
		public Observable<T> call(final R[] slice) {

			try {
				return store.fetch(database, table, slice).build().lift(mapper(mapper));
			} catch (final Exception e) {
				return Observable.error(e);
			}

		}

	}

	protected class ColumnRangeLoader<R extends Comparable<R>, C extends Comparable<C>, V, T, M extends ColumnListMapper<R, C, T>>
			implements Func0<Observable<T>> {

		private final Table<R, C, V> table;
		private final Class<M> mapper;
		private final R key;
		private final C start;
		private final C end;

		public ColumnRangeLoader(final Table<R, C, V> table_, final Class<M> mapper_, final R key_, final C start_,
				final C end_) {

			table = table_;
			mapper = mapper_;
			key = key_;
			start = start_;
			end = end_;

		}

		@SuppressWarnings("unchecked")
		@Override
		public Observable<T> call() {

			try {
				return store.fetch(database, table, key).start(start).end(end).build().lift(mapper(mapper));
			} catch (final Exception e) {
				return Observable.error(e);
			}

		}

	}

	protected class ColumnSliceLoader<R extends Comparable<R>, C extends Comparable<C>, V, T, M extends ColumnListMapper<R, C, T>>
			implements Func0<Observable<T>> {

		private final Table<R, C, V> table;
		private final Class<M> mapper;
		private final R key;
		private final int count;
		private final boolean reverse;

		public ColumnSliceLoader(final Table<R, C, V> table_, final Class<M> mapper_, final R key_, final int count_,
				final boolean reverse_) {

			table = table_;
			mapper = mapper_;
			key = key_;
			count = count_;
			reverse = reverse_;

		}

		@SuppressWarnings("unchecked")
		@Override
		public Observable<T> call() {

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

	}

	protected class ColumnPrefixLoader<R extends Comparable<R>, C extends Comparable<C>, V, T, M extends ColumnListMapper<R, C, T>>
			implements Func0<Observable<T>> {

		private final Table<R, C, V> table;
		private final Class<M> mapper;
		private final R key;
		private final String prefix;

		public ColumnPrefixLoader(final Table<R, C, V> table_, final Class<M> mapper_, final R key_,
				final String prefix_) {

			table = table_;
			mapper = mapper_;
			key = key_;
			prefix = prefix_;

		}

		@SuppressWarnings("unchecked")
		@Override
		public Observable<T> call() {

			try {
				return store.fetch(database, table, key).prefix(prefix).build().lift(mapper(mapper));
			} catch (final Exception e) {
				return Observable.error(e);
			}

		}

	}

	protected class ColumnKeyLoader<R extends Comparable<R>, C extends Comparable<C>, V, T, M extends ColumnListMapper<R, C, T>>
			implements Func1<C[], Observable<T>> {

		private final Table<R, C, V> table;
		private final Class<M> mapper;
		private final R key;

		public ColumnKeyLoader(final Table<R, C, V> table_, final Class<M> mapper_, final R key_) {
			table = table_;
			mapper = mapper_;
			key = key_;
		}

		@Override
		public Observable<T> call(final C[] columns) {

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

	}

}
