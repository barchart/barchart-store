package com.barchart.store.util;

import rx.util.functions.Func1;

import com.barchart.store.api.StoreRow;

public class EmptyRowFilter<K> implements Func1<StoreRow<K>, Boolean> {

	@Override
	public Boolean call(final StoreRow<K> row) {
		return row.columns().size() > 0;
	}

}
