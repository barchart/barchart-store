package com.barchart.store.heap;

import java.util.Collection;

import rx.Observable;
import rx.Subscriber;

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

}
