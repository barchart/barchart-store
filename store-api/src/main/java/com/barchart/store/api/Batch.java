package com.barchart.store.api;


/**
 * Fluent interface for updating rows in the data store.
 */
public interface Batch {

	/**
	 * Start a new row operation for this batch.
	 */
	<R extends Comparable<R>, C extends Comparable<C>, V> RowMutator<C> row(Table<R, C, V> table, R key)
			throws Exception;

	/**
	 * Commit all row operations from row() calls.
	 */
	void commit() throws Exception;

}
