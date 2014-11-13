package com.barchart.store.api;

import rx.Observable;

public interface ObservableQueryBuilder<R extends Comparable<R>, C extends Comparable<C>> {

	/**
	 * Return only the first <i>limit</i> columns for each row.
	 *
	 * @deprecated Use ObservableQueryBuilder#limit(limit)
	 */
	@Deprecated
	ObservableQueryBuilder<R, C> first(int limit);

	/**
	 * Return only the last <i>limit</i> columns for each row. This implicitly
	 * returns columns in descending order.
	 *
	 * @deprecated Use ObservableQueryBuilder#reverse(true).limit(limit)
	 */
	@Deprecated
	ObservableQueryBuilder<R, C> last(int limit);

	/**
	 * Return only the first <i>limit</i> columns for each row.
	 */
	ObservableQueryBuilder<R, C> limit(int limit);

	/**
	 * Reverse the order columns are returned in.
	 */
	ObservableQueryBuilder<R, C> reverse(boolean reversed);

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