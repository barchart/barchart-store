package com.barchart.store.util;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable.Operator;
import rx.Subscriber;

import com.barchart.store.api.RowMutator;
import com.barchart.store.api.StoreColumn;
import com.barchart.store.api.StoreRow;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class ColumnListMapper<R extends Comparable<R>, C extends Comparable<C>, T> implements
		Operator<T, StoreRow<R, C>> {

	private final Logger log = LoggerFactory.getLogger(getClass());

	@Override
	public Subscriber<? super StoreRow<R, C>> call(final Subscriber<? super T> subscriber) {

		return new Subscriber<StoreRow<R, C>>(subscriber) {

			@Override
			public void onNext(final StoreRow<R, C> row) {

				try {

					for (final C col : row.columns()) {

						if (subscriber.isUnsubscribed()) {
							return;
						}

						subscriber.onNext(decode(row.get(col)));

					}

				} catch (final Exception e) {
					log.error("{}", e);
					onError(e);
				}

			}

			@Override
			public void onCompleted() {
				subscriber.onCompleted();
			}

			@Override
			public void onError(final Throwable e) {
				subscriber.onError(e);
			}

		};

	}

	protected abstract void encode(T obj, RowMutator<C> mutator) throws Exception;

	protected abstract T decode(StoreColumn<C> column) throws Exception;

	public List<T> decode(final StoreRow<R, C> row) throws Exception {

		final List<T> records = new ArrayList<T>();

		for (int i = 0; i < row.size(); i++) {
			records.add(decode(row.getByIndex(i)));
		}

		return records;

	}

	protected ObjectMapper mapper() {
		return StoreObjectMapper.mapper;
	}

}
