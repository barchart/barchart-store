package com.barchart.store.api;

import java.nio.ByteBuffer;
import java.util.Date;

/**
 * A single column in a row.
 * 
 * @param <T>
 *            The column name/key data type
 */
public interface StoreColumn<T> {

	/**
	 * The column name (key).
	 */
	T getName();

	/**
	 * Get this column value as a string.
	 */
	String getString() throws Exception;

	/**
	 * Get this column value as a double.
	 */
	Double getDouble() throws Exception;

	/**
	 * Get this column value as an integer.
	 */
	Integer getInt() throws Exception;

	/**
	 * Get this column value as a long.
	 */
	Long getLong() throws Exception;

	/**
	 * Get this column value as a boolean.
	 */
	Boolean getBoolean() throws Exception;

	/**
	 * Get this column value as a date.
	 */
	Date getDate() throws Exception;

	/**
	 * Get this column value as a byte buffer.
	 */
	ByteBuffer getBlob() throws Exception;

	/**
	 * Get the last updated timestamp for this column.
	 */
	long getTimestamp() throws Exception;

}
