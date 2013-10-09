package com.barchart.store.api;

import java.nio.ByteBuffer;
import java.util.Date;

public interface StoreColumn<T> {

	T getName();

	String getString() throws Exception;

	Double getDouble() throws Exception;

	Integer getInt() throws Exception;

	Long getLong() throws Exception;

	long getTimestamp() throws Exception;

	Boolean getBoolean() throws Exception;

	Date getDate() throws Exception;

	ByteBuffer getBlob() throws Exception;
}
