package com.barchart.store.model.api;

import java.nio.ByteBuffer;

/**
 * A value stored as a blob with a type prefix. Should be preferred over
 * DynamicallyTypedValue since this can only be read as the type that was
 * storied initially, providing some type safety and allows clients to be
 * type-ignorant at query time.
 */
public interface StaticTypedValue<T extends StaticTypedValue<T>> {

	/**
	 * Set the underlying field value, embedding the value type for future
	 * decoding.
	 */
	<V> T set(V value);

	/**
	 * Get the field value as the stored type.
	 */
	Object get();

	/**
	 * The stored type encoded in the field value.
	 */
	Class<?> type();

	/**
	 * The serialized blob format of this value.
	 */
	ByteBuffer blob();

}
