package com.barchart.store.api;

import com.barchart.store.api.StoreService.Table;

/**
 * Fluent interface for updating rows in the data store.
 */
public interface Batch {

	/**
	 * Start a new row operation for this batch.
	 */
	<K, V> RowMutator<K> row(Table<K, V> table, String key) throws Exception;

	/**
	 * Commit all row operations from row() calls.
	 */
	void commit() throws Exception;

}
