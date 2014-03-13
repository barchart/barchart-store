package com.barchart.store.api;

import java.util.Collection;

/**
 * A single row from the data store, containing multiple columns of data.
 *
 * @param <T>
 *            The column key data type
 */
public interface StoreRow<T> extends Comparable<StoreRow<T>> {

	/**
	 * The unique row key.
	 */
	public String getKey();

	/**
	 * A list of column keys contained in this row.
	 */
	public Collection<T> columns();

	/**
	 * Get a column by index.
	 */
	public StoreColumn<T> getByIndex(int index);

	/**
	 * Get a column by name.
	 */
	public StoreColumn<T> get(T name);

}
