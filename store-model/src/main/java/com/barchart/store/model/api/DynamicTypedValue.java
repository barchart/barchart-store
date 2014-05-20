package com.barchart.store.model.api;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;

/**
 * A value stored as a blob that can be read as any arbitrary type.
 */
public interface DynamicTypedValue<T extends DynamicTypedValue<T>> {

	/**
	 * Set a String value.
	 */
	T set(String value);

	/**
	 * Set a String list. Each String value should not exceed 65,535 bytes.
	 */
	T set(List<String> value);

	/**
	 * Set a boolean value.
	 */
	T set(Boolean value);

	/**
	 * Set a long value.
	 */
	T set(Long value);

	/**
	 * Set a integer value.
	 */
	T set(Integer value);

	/**
	 * Set a double value.
	 */
	T set(Double value);

	/**
	 * Set a Date value.
	 */
	T set(Date value);

	/**
	 * Set a ByteBuffer value.
	 */
	T set(ByteBuffer value);

	/**
	 * Get the permission value as a String.
	 */
	String asString();

	/**
	 * Get the permission value as a list of Strings.
	 */
	List<String> asStringList();

	/**
	 * Get the permission value as a boolean.
	 */
	Boolean asBoolean();

	/**
	 * Get the permission value as a long.
	 */
	Long asLong();

	/**
	 * Get the permission value as a integer.
	 */
	Integer asInt();

	/**
	 * Get the permission value as a double.
	 */
	Double asDouble();

	/**
	 * Get the permission value as a Date.
	 */
	Date asDate();

	/**
	 * Get the raw permission value as a ByteBuffer.
	 */
	ByteBuffer asBlob();

}
