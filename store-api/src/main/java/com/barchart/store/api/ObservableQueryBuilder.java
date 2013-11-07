package com.barchart.store.api;

import rx.Observable;

public interface ObservableQueryBuilder<T> {

	// Max number of columns to return

	/**
	 * Return only the first <limit> columns for each row.
	 */
	ObservableQueryBuilder<T> first(int limit);

	/**
	 * Return only the last <limit> columns for each row.
	 */
	ObservableQueryBuilder<T> last(int limit);

	// Column range start/end by named columns

	/**
	 * Return a range of columns in comparator order, starting with the named
	 * column.
	 */
	ObservableQueryBuilder<T> start(T column);

	/**
	 * Return a range of columns in comparator order, ending with the named
	 * column.
	 */
	ObservableQueryBuilder<T> end(T column);

	/**
	 * Only return columns that start with the given prefix. Only provides
	 * predictable results for String column keys.
	 */
	ObservableQueryBuilder<T> prefix(String prefix);

	/**
	 * Only return the named columns for each row.
	 */
	ObservableQueryBuilder<T> columns(T... columns);

	/**
	 * Fetch all matching rows
	 */
	Observable<StoreRow<T>> build();

	/**
	 * Fetch a maximum of <limit> matching rows
	 */
	Observable<StoreRow<T>> build(int limit);

	/**
	 * Fetch a maximum of <limit> matching rows using the specified query batch
	 * size. Similar to a cursor query. For all rows use limit = 0.
	 */
	Observable<StoreRow<T>> build(int limit, int batchSize);

}