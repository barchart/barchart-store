package com.barchart.store.api;

import java.nio.ByteBuffer;

public interface RowMutator<T> {

	RowMutator<T> set(T column, String value) throws Exception;

	RowMutator<T> set(T column, double value) throws Exception;

	RowMutator<T> set(T column, ByteBuffer value) throws Exception;

	RowMutator<T> set(T column, int value) throws Exception;

	RowMutator<T> set(T column, long value) throws Exception;

	RowMutator<T> remove(T column) throws Exception;

	RowMutator<T> ttl(Integer ttl) throws Exception;

	void delete() throws Exception;
}
