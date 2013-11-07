package com.barchart.store.heap;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import rx.Observable;
import rx.Observer;
import rx.Subscription;

import com.barchart.store.api.StoreRow;

public class HeapQueryBuilder<T> extends QueryBuilderBase<T> {

	protected final Collection<HeapRow<T>> rows;

	public HeapQueryBuilder(final Collection<HeapRow<T>> rows_) {
		rows = rows_;
	}

	@Override
	public Observable<StoreRow<T>> build() {
		return build(0);
	}

	@Override
	public Observable<StoreRow<T>> build(final int limit) {

		return Observable.create(new Observable.OnSubscribeFunc<StoreRow<T>>() {

			@Override
			public Subscription onSubscribe(
					final Observer<? super StoreRow<T>> o) {

				final AtomicBoolean running = new AtomicBoolean(true);
				int ct = 0;

				try {

					for (final HeapRow<T> row : rows) {
						if (!running.get() || (limit > 0 && ct >= limit)) {
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

}
