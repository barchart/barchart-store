package com.barchart.store.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable.Operator;
import rx.Subscriber;

import com.barchart.store.api.RowMutator;
import com.barchart.store.api.StoreRow;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class RowMapper<R extends Comparable<R>, C extends Comparable<C>, T> implements
		Operator<T, StoreRow<R, C>> {

	private final Logger log = LoggerFactory.getLogger(getClass());

	@Override
	public Subscriber<? super StoreRow<R, C>> call(final Subscriber<? super T> subscriber) {

		return new Subscriber<StoreRow<R, C>>(subscriber) {

			@Override
			public void onNext(final StoreRow<R, C> row) {
				try {
					// Skip empty rows
					if (row.columns().size() > 0) {
						subscriber.onNext(decode(row));
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

	public abstract void encode(final T obj, final RowMutator<C> mutator) throws Exception;

	public abstract T decode(final StoreRow<R, C> row) throws Exception;

	protected ObjectMapper mapper() {
		return StoreObjectMapper.mapper;
	}

}
