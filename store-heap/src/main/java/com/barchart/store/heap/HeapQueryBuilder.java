package com.barchart.store.heap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicBoolean;

import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.util.functions.Func1;

import com.barchart.store.api.ObservableQueryBuilder;
import com.barchart.store.api.StoreColumn;
import com.barchart.store.api.StoreRow;

public class HeapQueryBuilder<T> implements ObservableQueryBuilder<T> {

	private final Collection<HeapRow<T>> rows;

	private Collection<T> columns = null;
	private T start = null;
	private T end = null;
	private int first = 0;
	private int last = 0;
	private String prefix = null;

	public HeapQueryBuilder(final Collection<HeapRow<T>> rows_) {
		rows = rows_;
	}

	@Override
	public ObservableQueryBuilder<T> first(final int limit) {
		first = limit;
		return this;
	}

	@Override
	public ObservableQueryBuilder<T> last(final int limit) {
		last = limit;
		return this;
	}

	@Override
	public ObservableQueryBuilder<T> start(final T column) {
		start = column;
		return this;
	}

	@Override
	public ObservableQueryBuilder<T> end(final T column) {
		end = column;
		return this;
	}

	@Override
	public ObservableQueryBuilder<T> prefix(final String prefix_) {
		prefix = prefix_;
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public ObservableQueryBuilder<T> columns(final T... columns_) {
		columns = new ArrayList<T>(Arrays.asList(columns_));
		return this;
	}

	@Override
	public Observable<StoreRow<T>> build() {
		return build(0);
	}

	@Override
	public Observable<StoreRow<T>> build(final int limit) {

		return Observable
				.create(new Func1<Observer<StoreRow<T>>, Subscription>() {

					@Override
					public Subscription call(final Observer<StoreRow<T>> o) {

						final AtomicBoolean running = new AtomicBoolean(true);
						int ct = 0;

						try {

							for (final HeapRow<T> row : rows) {
								if (!running.get()
										|| (limit > 0 && ct >= limit)) {
									o.onCompleted();
									break;
								}
								o.onNext(new RowFilter(row));
								ct++;
							}

							o.onCompleted();

						} catch (final Exception e) {
							o.onError(e);
						}

						return new Subscription() {

							@Override
							public void unsubscribe() {
								running.set(false);
							}

						};

					}

				});

	}

	@Override
	public Observable<StoreRow<T>> build(final int limit, final int batchSize) {
		return build(limit);
	}

	private class RowFilter implements StoreRow<T> {

		private final HeapRow<T> row;
		private final List<T> availColumns;

		public RowFilter(final HeapRow<T> row_) {
			row = row_;
			availColumns = new ArrayList<T>();
			buildColumnList();
		}

		private void buildColumnList() {

			if (first > 0) {

				int index = 0;
				final Iterator<T> iter = row.columns().iterator();

				while (iter.hasNext() && index < first) {
					availColumns.add(iter.next());
					index++;
				}

			} else if (last > 0) {

				int index = 0;
				final int offset = row.columns().size() - last;
				final Iterator<T> iter = row.columns().iterator();

				while (iter.hasNext()) {
					if (index < offset) {
						index++;
						iter.next();
						continue;
					}
					availColumns.add(iter.next());
				}

			} else if (start != null || end != null) {

				final SortedSet<T> rc = row.columnsImpl();
				if (start == null) {
					start = rc.first();
				} else if (end == null) {
					end = rc.last();
				}

				availColumns.addAll(rc.subSet(start, end));
				// subSet is tail-exclusive
				if (rc.contains(end)) {
					availColumns.add(end);
				}

			} else if (prefix != null) {

				final Iterator<T> iter = row.columns().iterator();

				while (iter.hasNext()) {
					final T name = iter.next();
					if (name.toString().startsWith(prefix)) {
						availColumns.add(name);
					}
				}

			} else if (columns != null) {
				columns.retainAll(row.columns());
				availColumns.addAll(columns);
			}

		}

		@Override
		public String getKey() {
			return row.getKey();
		}

		@Override
		public Collection<T> columns() {
			return Collections.unmodifiableCollection(availColumns);
		}

		@Override
		public StoreColumn<T> getByIndex(final int index) {

			if (availColumns.size() > 0) {

				if (index > availColumns.size() - 1) {
					throw new ArrayIndexOutOfBoundsException();
				}

				return row.get(availColumns.get(index));

			} else {
				return row.getByIndex(index);
			}

		}

		@Override
		public StoreColumn<T> get(final T name) {
			if (availColumns.size() == 0 || availColumns.contains(name)) {
				return row.get(name);
			}
			return null;
		}

	}

}
