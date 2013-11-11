package com.barchart.store.util;

import java.util.Arrays;
import java.util.List;

import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.util.functions.Func1;

import com.barchart.store.api.Batch;
import com.barchart.store.api.ObservableIndexQueryBuilder;
import com.barchart.store.api.ObservableIndexQueryBuilder.Operator;
import com.barchart.store.api.ObservableQueryBuilder;
import com.barchart.store.api.RowMutator;
import com.barchart.store.api.StoreService;
import com.barchart.store.api.StoreService.Table;

public abstract class StoreObjectMapper {

	private final StoreService store;
	private final String database;
	private final StoreSchema schema;
	private final MapperFactory mappers;

	public StoreObjectMapper(final StoreService store_, final String database_,
			final StoreSchema schema_) {
		store = store_;
		database = database_;
		schema = schema_;
		mappers = new MapperFactory();
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

	protected <K, T, M extends RowMapper<K, T>> M mapper(final Class<M> cls) {
		return mappers.instance(cls);
	}

	/*
	 * Helper methods for subclasses.
	 */

	protected <K, V, T, M extends RowMapper<K, T>> Observable<T> loadRows(
			final Table<K, V> table, final Class<M> mapper,
			final String... keys) {

		try {
			return store.fetch(database, table, keys).build()
					.filter(new EmptyRowFilter<K>()).map(mapper(mapper));
		} catch (final Exception e) {
			return Observable.error(e);
		}

	}

	protected <K, V, T, M extends RowMapper<K, List<T>>> Observable<T> loadColumns(
			final Table<K, V> table, final Class<M> mapper, final String key,
			final K... columns) {

		try {

			final ObservableQueryBuilder<K> query =
					store.fetch(database, table, key);

			if (columns != null && columns.length > 0) {
				query.columns(columns);
			}

			return query.build().filter(new EmptyRowFilter<K>())
					.map(mapper(mapper)).mapMany(new ListExploder<T>());

		} catch (final Exception e) {
			return Observable.error(e);
		}

	}

	protected <V, T, M extends RowMapper<String, List<T>>> Observable<T> loadColumnsByPrefix(
			final Table<String, V> table, final Class<M> mapper,
			final String key, final String prefix) {

		try {

			return store.fetch(database, table, key).prefix(prefix).build()
					.filter(new EmptyRowFilter<String>()).map(mapper(mapper))
					.mapMany(new ListExploder<T>());

		} catch (final Exception e) {
			return Observable.error(e);
		}

	}

	protected <K, V, T, M extends RowMapper<K, List<T>>> Observable<T> loadColumns(
			final Table<K, V> table, final Class<M> mapper, final String key,
			final int count, final boolean reverse) {

		try {

			final ObservableQueryBuilder<K> query =
					store.fetch(database, table, key);

			if (reverse) {
				query.last(count);
			} else {
				query.first(count);
			}

			return query.build().filter(new EmptyRowFilter<K>())
					.map(mapper(mapper)).mapMany(new ListExploder<T>());

		} catch (final Exception e) {
			return Observable.error(e);
		}

	}

	protected <K, V, T, M extends RowMapper<K, List<T>>> Observable<T> loadColumns(
			final Table<K, V> table, final Class<M> mapper, final String key,
			final K start, final K end) {

		try {

			return store.fetch(database, table, key).start(start).end(end)
					.build().filter(new EmptyRowFilter<K>())
					.map(mapper(mapper)).mapMany(new ListExploder<T>());

		} catch (final Exception e) {
			return Observable.error(e);
		}

	}

	protected <K, V, T, M extends RowMapper<K, T>> Observable<T> findRows(
			final Table<K, V> table, final Class<M> mapper,
			final Where<K>... clauses) {

		try {

			final ObservableIndexQueryBuilder<K> builder =
					store.query(database, table);

			for (final Where<K> where : clauses) {
				builder.where(where.field, where.value, where.operator);
			}

			return builder.build().filter(new EmptyRowFilter<K>())
					.map(mapper(mapper));

		} catch (final Exception e) {
			return Observable.error(e);
		}

	}

	protected <K, V, T, M extends RowMapper<K, T>> Observable<T> createRow(
			final Table<K, V> table, final Class<M> mapper, final String key,
			final T obj) {

		try {

			return Observable.create(new Observable.OnSubscribeFunc<T>() {

				@Override
				public Subscription onSubscribe(
						final Observer<? super T> observer) {

					try {

						loadRows(table, mapper, key).subscribe(
								new ExistenceObserver<T>(observer) {

									@Override
									public void exists(
											final Observer<? super T> observer) {
										observer.onError(new Exception(
												"Object ID already exists"));
									}

									@Override
									public void missing(
											final Observer<? super T> observer) {
										updateRow(table, mapper, key, obj)
												.subscribe(observer);
									}

								});

					} catch (final Throwable t) {
						observer.onError(t);
					}

					return new Subscription() {

						@Override
						public void unsubscribe() {
						}

					};

				}

			});

		} catch (final Exception e) {
			return Observable.error(e);
		}

	}

	protected <K, V, T, M extends RowMapper<K, T>> Observable<T> updateRow(
			final Table<K, V> table, final Class<M> mapper, final String key,
			final T obj) {

		try {

			final Batch batch = store.batch(database);
			final RowMutator<K> mutator = batch.row(table, key);

			mapper(mapper).encode(obj, mutator);
			batch.commit();

			return Observable.from(obj);

		} catch (final Exception e) {
			return Observable.error(e);
		}

	}

	protected <K, V, T, M extends RowMapper<K, List<T>>> Observable<T> updateColumns(
			final Table<K, V> table, final Class<M> mapper, final String key,
			final T... objects) {

		try {

			final Batch batch = store.batch(database);
			final RowMutator<K> mutator = batch.row(table, key);
			mapper(mapper).encode(Arrays.asList(objects), mutator);
			batch.commit();

			return Observable.from(objects);

		} catch (final Exception e) {
			return Observable.error(e);
		}

	}

	protected <K, V> Observable<String> deleteRows(final Table<K, V> table,
			final String... keys) {

		try {

			final Batch batch = store.batch(database);
			for (final String key : keys) {
				batch.row(table, key).delete();
			}
			batch.commit();

			return Observable.from(keys);

		} catch (final Exception e) {
			return Observable.error(e);
		}

	}

	protected <K, V> Observable<K> deleteColumns(final Table<K, V> table,
			final String key, final K... columns) {

		try {

			final Batch batch = store.batch(database);
			final RowMutator<K> row = batch.row(table, key);

			for (final K column : columns) {
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

	protected static String[] toStrings(final List<?> list) {

		final String[] strings = new String[list.size()];

		for (int i = 0; i < list.size(); i++) {
			strings[i] = list.get(i).toString();
		}

		return strings;

	}

	private static class ListExploder<T> implements
			Func1<List<T>, Observable<T>> {

		@Override
		public Observable<T> call(final List<T> t1) {
			return Observable.from(t1);
		}

	}

	protected static class Where<T> {

		private final T field;
		private final Object value;
		private final Operator operator;

		private Where(final T field_, final Object value_,
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
