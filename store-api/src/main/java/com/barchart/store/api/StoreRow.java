package com.barchart.store.api;

import java.util.Collection;

public interface StoreRow<T> {

	public String getKey();

	public Collection<T> columns();

	public StoreColumn<T> getByIndex(int index);

	public StoreColumn<T> get(T name);

}
