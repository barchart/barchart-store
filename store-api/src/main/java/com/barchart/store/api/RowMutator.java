package com.barchart.store.api;

import java.nio.ByteBuffer;

/**
 * A batch row operation for updating rows in the data store.
 *
 * @param <T>
 *            The column key type for this row
 */
public interface RowMutator<T> {

	/**
	 * Set a string column value.
	 */
	RowMutator<T> set(T column, String value);

	/**
	 * Set a double column value.
	 */
	RowMutator<T> set(T column, double value);

	/**
	 * Set a ByteBuffer column value.
	 */
	RowMutator<T> set(T column, ByteBuffer value);

	/**
	 * Set an int column value.
	 */
	RowMutator<T> set(T column, int value);

	/**
	 * Set a long column value.
	 */
	RowMutator<T> set(T column, long value);

	/**
	 * Remove a column from this row.
	 */
	RowMutator<T> remove(T column);

	/**
	 * Set the time-to-live in seconds for all subsequent value set() calls.
	 */
	RowMutator<T> ttl(Integer ttl);

	/**
	 * Delete this row.
	 */
	void delete();

}
