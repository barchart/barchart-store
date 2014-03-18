package com.barchart.store.util;

import rx.util.functions.Func1;

import com.barchart.store.api.StoreRow;

public class EmptyRowFilter<R extends Comparable<R>, C extends Comparable<C>> implements Func1<StoreRow<R, C>, Boolean> {

	@Override
	public Boolean call(final StoreRow<R, C> row) {
		return row.columns().size() > 0;
	}

}
