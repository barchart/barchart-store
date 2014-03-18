package com.barchart.store.heap;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import rx.Observable;
import rx.Observer;
import rx.Subscription;

import com.barchart.store.api.StoreRow;

public class HeapQueryBuilder<R extends Comparable<R>, C extends Comparable<C>> extends QueryBuilderBase<R, C> {

	protected final Collection<HeapRow<R, C>> rows;

	public HeapQueryBuilder(final Collection<HeapRow<R, C>> rows_) {
		rows = rows_;
	}

	@Override
	public Observable<StoreRow<R, C>> build() {
		return build(0);
	}

	@Override
	public Observable<StoreRow<R, C>> build(final int limit) {

		return Observable.create(new Observable.OnSubscribeFunc<StoreRow<R, C>>() {

			@Override
			public Subscription onSubscribe(
					final Observer<? super StoreRow<R, C>> o) {

				final AtomicBoolean running = new AtomicBoolean(true);
				int ct = 0;

				try {

					for (final HeapRow<R, C> row : rows) {
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
	public Observable<StoreRow<R, C>> build(final int limit, final int batchSize) {
		return build(limit);
	}

}
