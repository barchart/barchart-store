package com.barchart.store.api;

import java.nio.ByteBuffer;
import java.util.Date;

/**
 * A single column in a row.
 *
 * @param <T>
 *            The column name/key data type
 */
public interface StoreColumn<T extends Comparable<T>> extends Comparable<StoreColumn<T>> {

	/**
	 * The column name (key).
	 */
	T getName();

	/**
	 * Get this column value as a string.
	 */
	String getString();

	/**
	 * Get this column value as a double.
	 */
	Double getDouble();

	/**
	 * Get this column value as an integer.
	 */
	Integer getInt();

	/**
	 * Get this column value as a long.
	 */
	Long getLong();

	/**
	 * Get this column value as a boolean.
	 */
	Boolean getBoolean();

	/**
	 * Get this column value as a date.
	 */
	Date getDate();

	/**
	 * Get this column value as a byte buffer.
	 */
	ByteBuffer getBlob();

	/**
	 * Get the last updated timestamp for this column.
	 */
	long getTimestamp();

}
