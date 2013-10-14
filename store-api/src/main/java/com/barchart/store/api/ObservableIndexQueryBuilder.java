package com.barchart.store.api;

public interface ObservableIndexQueryBuilder<T> extends
		ObservableQueryBuilder<T> {

	public enum Operator {
		EQUAL, GT, GTE, LT, LTE
	};

	/**
	 * Filter rows based on column value (using Operator.EQUAL).
	 */
	ObservableIndexQueryBuilder<T> where(T column, Object value);

	/**
	 * Filter rows based on column value using the specific comparison operator.
	 */
	ObservableIndexQueryBuilder<T> where(T column, Object value, Operator op);

}