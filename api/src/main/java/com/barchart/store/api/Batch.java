package com.barchart.store.api;

import rx.Observable;

/**
 * Fluent interface for updating rows in the data store.
 */
public interface Batch {

	/**
	 * Start a new row operation for this batch.
	 */
	<R extends Comparable<R>, C extends Comparable<C>, V> RowMutator<C> row(Table<R, C, V> table, R key);

	/**
	 * Commit all row operations from row() calls.
	 */
	Observable<Boolean> commit();

}
