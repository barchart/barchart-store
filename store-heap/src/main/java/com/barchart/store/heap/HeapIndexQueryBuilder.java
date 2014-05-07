package com.barchart.store.heap;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import rx.Observable;
import rx.Subscriber;

import com.barchart.store.api.ObservableIndexQueryBuilder;
import com.barchart.store.api.StoreColumn;
import com.barchart.store.api.StoreRow;

public class HeapIndexQueryBuilder<R extends Comparable<R>, C extends Comparable<C>> extends QueryBuilderBase<R, C>
		implements ObservableIndexQueryBuilder<R, C> {

	protected final Collection<HeapRow<R, C>> rows;
	final Map<C, Map<Object, Collection<HeapRow<R, C>>>> indexes;
	final List<FieldCompare> filters;

	public HeapIndexQueryBuilder(final Map<C, Map<Object, Collection<HeapRow<R, C>>>> indexes_) {
		indexes = indexes_;
		filters = new ArrayList<FieldCompare>();
		if (indexes == null) {
			rows = Collections.<HeapRow<R, C>> emptySet();
		} else {
			rows = new HashSet<HeapRow<R, C>>();
		}
	}

	@Override
	public ObservableIndexQueryBuilder<R, C> where(final C column,
			final Object value) {
		return where(column, value, Operator.EQUAL);
	}

	@Override
	public ObservableIndexQueryBuilder<R, C> where(final C column,
			final Object value,
			final com.barchart.store.api.ObservableIndexQueryBuilder.Operator op) {
		filters.add(new FieldCompare(column, op, value));
		return this;
	}

	@Override
	public Observable<StoreRow<R, C>> build() {
		return build(0);
	}

	@Override
	public Observable<StoreRow<R, C>> build(final int limit) {

		if (indexes != null) {

			boolean validQuery = (filters.size() == 0);

			for (final FieldCompare fc : filters) {
				if (fc.operator == Operator.EQUAL) {
					validQuery = true;
					break;
				}
			}

			if (!validQuery) {
				throw new IllegalArgumentException(
						"Secondary index queries must contain at least one EQUAL term");
			}

			// This shit is really inefficient, avoid LT/GT filters on large
			// data sets for StoreHeap
			boolean first = true;

			for (final FieldCompare fc : filters) {

				final Iterator<HeapRow<R, C>> iter;

				switch (fc.operator) {

					case GT:
						iter = rows.iterator();
						while (iter.hasNext()) {
							final HeapRow<R, C> row = iter.next();
							try {
								if (compare(fc.value, row.get(fc.column)) <= 0) {
									iter.remove();
								}
							} catch (final Exception e) {
								iter.remove();
							}
						}
						break;

					case GTE:
						iter = rows.iterator();
						while (iter.hasNext()) {
							final HeapRow<R, C> row = iter.next();
							try {
								if (compare(fc.value, row.get(fc.column)) < 0) {
									iter.remove();
								}
							} catch (final Exception e) {
								iter.remove();
							}
						}
						break;

					case LT:
						iter = rows.iterator();
						while (iter.hasNext()) {
							final HeapRow<R, C> row = iter.next();
							try {
								if (compare(fc.value, row.get(fc.column)) >= 0) {
									iter.remove();
								}
							} catch (final Exception e) {
								iter.remove();
							}
						}
						break;

					case LTE:
						iter = rows.iterator();
						while (iter.hasNext()) {
							final HeapRow<R, C> row = iter.next();
							try {
								if (compare(fc.value, row.get(fc.column)) > 0) {
									iter.remove();
								}
							} catch (final Exception e) {
								iter.remove();
							}
						}
						break;

					case EQUAL:
					default:

						Collection<HeapRow<R, C>> matches =
								indexes.containsKey(fc.column) ? matches =
										indexes.get(fc.column).get(fc.value)
										: null;

						if (rows.size() == 0 && first) {
							if (matches != null) {
								rows.addAll(matches);
							}
						} else {
							if (matches != null) {
								rows.retainAll(matches);
							} else {
								rows.clear();
							}
						}

				}

				first = false;

			}

		}

		return Observable.create(new Observable.OnSubscribe<StoreRow<R, C>>() {

			@Override
			public void call(final Subscriber<? super StoreRow<R, C>> subscriber) {

				int ct = 0;

				for (final HeapRow<R, C> row : rows) {

					if (subscriber.isUnsubscribed()) {
						return;
					}

					if (limit > 0 && ct >= limit) {
						subscriber.onCompleted();
						break;
					}

					subscriber.onNext(new QueryRow(row));
					ct++;

				}

				subscriber.onCompleted();

			}

		});

	}

	@Override
	public Observable<StoreRow<R, C>> build(final int limit, final int batchSize) {
		return build(limit);
	}

	private int compare(final Object o1, final StoreColumn<C> o2)
			throws Exception {

		if (o1.getClass() == String.class) {
			return ((String) o1).compareTo(o2.getString());
		} else if (o1.getClass() == byte[].class) {
			return ByteBuffer.wrap((byte[]) o1).compareTo(o2.getBlob());
		} else if (o1.getClass() == Boolean.class) {
			if (!o1.equals(o2.getBoolean())) {
				return (Boolean) o1 ? 1 : -1;
			}
		} else if (o1.getClass() == ByteBuffer.class) {
			return ((ByteBuffer) o1).compareTo(o2.getBlob());
		} else if (o1.getClass() == Double.class) {
			return ((Double) o1).compareTo(o2.getDouble());
		} else if (o1.getClass() == Integer.class) {
			return ((Integer) o1).compareTo(o2.getInt());
		} else if (o1.getClass() == Long.class) {
			return ((Long) o1).compareTo(o2.getLong());
		} else if (o1.getClass() == Date.class) {
			return ((Date) o1).compareTo(o2.getDate());
		}

		return 0;

	}

	private class FieldCompare {
		public C column;
		public Operator operator;
		public Object value;

		public FieldCompare(final C column_, final Operator operator_,
				final Object value_) {
			column = column_;
			operator = operator_;
			value = value_;
		}
	}

}
