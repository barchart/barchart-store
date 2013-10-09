package com.barchart.store.api;

import com.barchart.store.api.StoreService.Table;

public interface Batch {

	<K, V> RowMutator<K> row(Table<K, V> table, String key) throws Exception;

	void commit() throws Exception;

}
