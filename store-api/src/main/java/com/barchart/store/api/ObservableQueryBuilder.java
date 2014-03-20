package com.barchart.store.api;

import rx.Observable;

public interface ObservableQueryBuilder<R extends Comparable<R>, C extends Comparable<C>> {

	// Max number of columns to return

	/**
	 * Return only the first <limit> columns for each row.
	 */
	ObservableQueryBuilder<R, C> first(int limit);

	/**
	 * Return only the last <limit> columns for each row. This implicitly calls
	 * reverse().
	 */
	ObservableQueryBuilder<R, C> last(int limit);

	/**
	 * Reverse the order columns are returned in.
	 */
	ObservableQueryBuilder<R, C> reverse(boolean reversed);

	// Column range start/end by named columns

	/**
	 * Return a range of columns in comparator order, starting with the named
	 * column.
	 */
	ObservableQueryBuilder<R, C> start(C column);

	/**
	 * Return a range of columns in comparator order, ending with the named
	 * column.
	 */
	ObservableQueryBuilder<R, C> end(C column);

	/**
	 * Only return columns that start with the given prefix. Only provides
	 * predictable results for String column keys.
	 */
	ObservableQueryBuilder<R, C> prefix(String prefix);

	/**
	 * Only return the named columns for each row.
	 */
	ObservableQueryBuilder<R, C> columns(C... columns);

	/**
	 * Fetch all matching rows
	 */
	Observable<StoreRow<R, C>> build();

	/**
	 * Fetch a maximum of <limit> matching rows
	 */
	Observable<StoreRow<R, C>> build(int limit);

	/**
	 * Fetch a maximum of <limit> matching rows using the specified query batch
	 * size. Similar to a cursor query. For all rows use limit = 0.
	 */
	Observable<StoreRow<R, C>> build(int limit, int batchSize);

}