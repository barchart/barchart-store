package com.barchart.store.api;

import java.util.Collection;

/**
 * A single row from the data store, containing multiple columns of data.
 *
 * @param <T>
 *            The column key data type
 */
public interface StoreRow<R extends Comparable<R>, C extends Comparable<C>> extends Comparable<StoreRow<R, C>> {

	/**
	 * The unique row key.
	 */
	public R getKey();

	/**
	 * A list of column keys contained in this row.
	 */
	public Collection<C> columns();

	/**
	 * Get a column by index.
	 */
	public StoreColumn<C> getByIndex(int index);

	/**
	 * Get a column by name.
	 */
	public StoreColumn<C> get(C name);

	/**
	 * Get the number of columns in this row result.
	 */
	public int size();

}
