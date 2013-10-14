package com.barchart.store.api;

import rx.Observable;

public interface ObservableQueryBuilder<T> {

	// Max number of columns to return

	ObservableQueryBuilder<T> first(int limit);

	ObservableQueryBuilder<T> last(int limit);

	// Column range start/end by named columns

	ObservableQueryBuilder<T> start(T column);

	ObservableQueryBuilder<T> end(T column);

	// Column slice by name(s)

	ObservableQueryBuilder<T> prefix(String prefix);

	ObservableQueryBuilder<T> columns(
			@SuppressWarnings("unchecked") T... columns);

	/*
	 * Fetch all matching rows
	 */
	Observable<StoreRow<T>> build();

	/*
	 * Fetch a maximum of <limit> matching rows
	 */
	Observable<StoreRow<T>> build(int limit);

	/*
	 * Fetch a maximum of <limit> matching rows using the specified query batch
	 * size. Similar to a cursor query. For all rows use limit = 0.
	 */
	Observable<StoreRow<T>> build(int limit, int batchSize);

}