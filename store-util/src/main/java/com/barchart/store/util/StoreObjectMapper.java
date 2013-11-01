package com.barchart.store.util;

import java.util.Arrays;
import java.util.List;

import com.barchart.store.api.Batch;
import com.barchart.store.api.ObservableIndexQueryBuilder;
import com.barchart.store.api.ObservableIndexQueryBuilder.Operator;
import com.barchart.store.api.ObservableQueryBuilder;
import com.barchart.store.api.RowMutator;
import com.barchart.store.api.StoreService;
import com.barchart.store.api.StoreService.Table;
import com.barchart.util.observer.Observer;

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

	public void updateSchema() throws Exception {
		schema.update(store, database);
	}

	protected <K, V, T, M extends RowMapper<K, T>> void loadRows(
			final Table<K, V> table, final Class<M> mapper,
			final Observer<T> observer, final String... keys) {

		try {

			store.fetch(database, table, keys).build()
					.filter(new EmptyRowFilter<K>()).map(mapper(mapper))
					.subscribe(new WrapObserver<T>(observer));

		} catch (final Throwable e) {
			observer.onError(e);
		}

	}

	protected <K, V, T, M extends RowMapper<K, List<T>>> void loadColumns(
			final Table<K, V> table, final Class<M> mapper,
			final Observer<T> observer, final String key, final K... columns) {

		try {

			final ObservableQueryBuilder<K> query =
					store.fetch(database, table, key);

			if (columns != null && columns.length > 0) {
				query.columns(columns);
			}

			query.build().filter(new EmptyRowFilter<K>()).map(mapper(mapper))
					.subscribe(new ListObserver<T>(observer));

		} catch (final Throwable e) {
			observer.onError(e);
		}

	}

	protected <V, T, M extends RowMapper<String, List<T>>> void loadColumnsByPrefix(
			final Table<String, V> table, final Class<M> mapper,
			final Observer<T> observer, final String key, final String prefix) {

		try {

			store.fetch(database, table, key).prefix(prefix).build()
					.filter(new EmptyRowFilter<String>()).map(mapper(mapper))
					.subscribe(new ListObserver<T>(observer));

		} catch (final Throwable e) {
			observer.onError(e);
		}

	}

	protected <K, V, T, M extends RowMapper<K, List<T>>> void loadColumns(
			final Table<K, V> table, final Class<M> mapper,
			final Observer<T> observer, final String key, final int count,
			final boolean reverse) {

		try {

			final ObservableQueryBuilder<K> query =
					store.fetch(database, table, key);

			if (reverse) {
				query.last(count);
			} else {
				query.first(count);
			}

			query.build().filter(new EmptyRowFilter<K>()).map(mapper(mapper))
					.subscribe(new ListObserver<T>(observer));

		} catch (final Throwable e) {
			observer.onError(e);
		}

	}

	protected <K, V, T, M extends RowMapper<K, List<T>>> void loadColumns(
			final Table<K, V> table, final Class<M> mapper,
			final Observer<T> observer, final String key, final K start,
			final K end) {

		try {

			store.fetch(database, table, key).start(start).end(end).build()
					.filter(new EmptyRowFilter<K>()).map(mapper(mapper))
					.subscribe(new ListObserver<T>(observer));

		} catch (final Throwable e) {
			observer.onError(e);
		}

	}

	protected <K, V, T, M extends RowMapper<K, T>> void findRows(
			final Table<K, V> table, final Class<M> mapper,
			final Observer<T> observer, final Where<K>... clauses) {

		try {

			final ObservableIndexQueryBuilder<K> builder =
					store.query(database, table);

			for (final Where<K> where : clauses) {
				builder.where(where.field, where.value, where.operator);
			}

			builder.build().filter(new EmptyRowFilter<K>()).map(mapper(mapper))
					.subscribe(new WrapObserver<T>(observer));

		} catch (final Throwable e) {
			observer.onError(e);
		}

	}

	protected <K, V, T, M extends RowMapper<K, T>> void createRow(
			final Table<K, V> table, final Class<M> mapper,
			final Observer<T> observer, final String key, final T obj) {

		try {

			loadRows(table, mapper, new Observer<T>() {

				private boolean found = false;

				@Override
				public void onNext(final T next) {
					found = true;
				}

				@Override
				public void onError(final Throwable error) {
					observer.onError(error);
				}

				@Override
				public void onCompleted() {
					if (!found) {
						try {
							updateRow(table, mapper, observer, key, obj);
						} catch (final Throwable e) {
							observer.onError(e);
						}
					} else {
						observer.onError(new IllegalStateException(
								"Row key already exists!"));
					}
				}

			}, key);

		} catch (final Throwable e) {
			observer.onError(e);
		}

	}

	protected <K, V, T, M extends RowMapper<K, T>> void updateRow(
			final Table<K, V> table, final Class<M> mapper,
			final Observer<T> observer, final String key, final T obj) {

		try {

			final Batch batch = store.batch(database);
			final RowMutator<K> mutator = batch.row(table, key);

			mapper(mapper).encode(obj, mutator);
			batch.commit();

			observer.onNext(obj);
			observer.onCompleted();

		} catch (final Throwable e) {
			observer.onError(e);
		}

	}

	protected <K, V, T, M extends RowMapper<K, List<T>>> void updateColumns(
			final Table<K, V> table, final Class<M> mapper,
			final Observer<T> observer, final String key, final T... objects) {

		try {

			final Batch batch = store.batch(database);
			final RowMutator<K> mutator = batch.row(table, key);
			mapper(mapper).encode(Arrays.asList(objects), mutator);
			batch.commit();

			for (final T object : objects) {
				observer.onNext(object);
			}
			observer.onCompleted();

		} catch (final Throwable e) {
			observer.onError(e);
		}

	}

	protected <K, V> void deleteRows(final Table<K, V> table,
			final Observer<String> observer, final String... keys) {

		try {

			final Batch batch = store.batch(database);
			for (final String key : keys) {
				batch.row(table, key).delete();
			}
			batch.commit();

			for (final String key : keys) {
				observer.onNext(key);
			}
			observer.onCompleted();

		} catch (final Throwable e) {
			observer.onError(e);
		}

	}

	protected <K, V> void deleteColumns(final Table<K, V> table,
			final Observer<K> observer, final String key, final K... columns) {

		try {

			final Batch batch = store.batch(database);
			final RowMutator<K> row = batch.row(table, key);

			for (final K column : columns) {
				row.remove(column);
			}

			batch.commit();

			for (final K column : columns) {
				observer.onNext(column);
			}

			observer.onCompleted();

		} catch (final Throwable e) {
			observer.onError(e);
		}

	}

	protected <K, T, M extends RowMapper<K, T>> M mapper(final Class<M> cls) {
		return mappers.instance(cls);
	}

	private static class WrapObserver<T> extends ChainObserver<T, T> {

		public WrapObserver(final Observer<T> observer_) {
			super(observer_);
		}

		@Override
		public void onNext(final T next) {
			observer().onNext(next);
		}

	}

	private static class ListObserver<T> extends ChainObserver<List<T>, T> {

		public ListObserver(final Observer<T> observer_) {
			super(observer_);
		}

		@Override
		public void onNext(final List<T> list) {
			for (final T t : list) {
				observer().onNext(t);
			}
		}

	}

	private abstract static class ChainObserver<T, U> implements rx.Observer<T> {

		private final Observer<U> observer;

		public ChainObserver(final Observer<U> observer_) {
			observer = observer_;
		}

		@Override
		public void onCompleted() {
			observer.onCompleted();
		}

		@Override
		public void onError(final Throwable e) {
			observer.onError(e);
		}

		public Observer<U> observer() {
			return observer;
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
