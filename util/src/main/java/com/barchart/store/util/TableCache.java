package com.barchart.store.util;

import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;

/**
 * Near-cache for decoded row objects from the store. Each cached table should have its own TableCache instance, since
 * the API does not provide information on the data's source table.
 */
public interface TableCache<R extends Comparable<R>, C extends Comparable<C>, T> {

	/**
	 * Query for cached row objects, loading from store if missing. Store results will be cached during response. This
	 * cache mechanism should be used if objects are decoded from an entire row.
	 */
	public Observable<T> rows(final Func1<R[], Observable<T>> loader, R... rows);

	/**
	 * Query for cached column objects, loading from store if missing. Store results will be cached during response.
	 * This cache mechanism should be used if each column is decoded as a separate object.
	 */
	public Observable<T> columns(final Func1<C[], Observable<T>> loader, R row, final C... columns);

	/**
	 * Query for cached column objects, loading from store if missing. Store results will be cached during response.
	 * This cache mechanism should be used if each column is decoded as a separate object.
	 */
	public Observable<T> columns(final Func0<Observable<T>> loader, R row, final C start, final C end);

	/**
	 * Query for cached column objects, loading from store if missing. Store results will be cached during response.
	 * This cache mechanism should be used if each column is decoded as a separate object.
	 */
	public Observable<T> columns(final Func0<Observable<T>> loader, R row, final int count,
			final boolean reverse);

	/**
	 * Query for cached column objects, loading from store if missing. Store results will be cached during response.
	 * This cache mechanism should be used if each column is decoded as a separate object.
	 */
	public Observable<T> columnsByPrefix(final Func0<Observable<T>> loader, R row, final String prefix);

	/**
	 * Update or invalidate the specified row object.
	 */
	public void update(R row, T object);

	/**
	 * Update or invalidate the specified column object.
	 */
	public void update(R row, C column, T object);

	/**
	 * Invalidate the specified row object.
	 */
	public void invalidate(R row);

	/**
	 * Invalidate the specified column object.
	 */
	public void invalidate(R row, C column);

}