package com.barchart.store.util;

import rx.util.functions.Func1;

import com.barchart.store.api.StoreRow;

/**
 * @see Filters#EMPTY_ROW for singleton
 */
public class EmptyRowFilter implements Func1<StoreRow<?, ?>, Boolean> {

	@Override
	public Boolean call(final StoreRow<?, ?> row) {
		return row.columns().size() > 0;
	}

}
