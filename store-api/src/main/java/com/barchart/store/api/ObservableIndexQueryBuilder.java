package com.barchart.store.api;

public interface ObservableIndexQueryBuilder<T> extends
		ObservableQueryBuilder<T> {

	public enum Operator {

		/**
		 * Equals operator.
		 */
		EQUAL,

		/**
		 * Greater-than operator.
		 */
		GT,

		/**
		 * Greater-than-or-equal operator.
		 */
		GTE,

		/**
		 * Less-than operator.
		 */
		LT,

		/**
		 * Less-than-or-equal operator.
		 */
		LTE

	};

	/**
	 * Filter rows based on column value (using Operator.EQUAL).
	 */
	ObservableIndexQueryBuilder<T> where(T column, Object value);

	/**
	 * Filter rows based on column value using the specified comparison
	 * operator.
	 */
	ObservableIndexQueryBuilder<T> where(T column, Object value, Operator op);

}