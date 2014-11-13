package com.barchart.store.api;

public interface ObservableIndexQueryBuilder<R extends Comparable<R>, C extends Comparable<C>> extends
		ObservableQueryBuilder<R, C> {

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
	ObservableIndexQueryBuilder<R, C> where(C column, Object value);

	/**
	 * Filter rows based on column value using the specified comparison
	 * operator.
	 */
	ObservableIndexQueryBuilder<R, C> where(C column, Object value, Operator op);

}