package com.barchart.store.api;

/**
 * Static column definition. Static columns are assumed to have string keys.
 * Wide rows should just add columns on the fly.
 * 
 * @author jeremy
 * 
 * @param <T>
 */
public interface ColumnDef {

	// Column key value
	String key();

	// Create secondary index
	boolean isIndexed();

	// Boolean, String, Integer, Long, Double, Date, byte[]
	Class<?> type();

}
