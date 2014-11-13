package com.barchart.store.api;

import rx.Observable;

/**
 * Data store interface for persisting key/value data of various types. Built to
 * support the typical features of modern distributed key/value stores
 * (Cassandra, Dynamo, Voldemort, Riak, etc).
 * 
 * Usage examples:
 * 
 * <pre>
 * // Create a database/keyspace
 * store.create("database");
 *
 * // Create a table (definition is typically cached/static)
 * Table<String, String, String> test = Table.builder("test").build();
 * store.create("database", test);
 *
 * // Update some data
 * Batch batch = store.batch("database", test);
 *
 * // Add or update a row
 * batch.row("new-row-key")
 *      .set("column1", "value1")
 *      .set("column2", "value2")
 *      .set("column3", 500);
 *
 * // Remove a column from a row
 * batch.row("update-row-key").remove("column4");
 *
 * // Delete a row
 * batch.row("key-to-delete").delete();
 *
 * // Commit all batch operations
 * batch.commit();
 *
 * // Fetch all columns in a row
 * store.fetch("database", test, "row-key").build().subscribe(observer);
 *
 * // Fetch multiple rows
 * store.fetch("database", test, "row-key-1", "row-key-2", "row-key-3")
 *      .build().subscribe(observer);
 *
 * // Fetch specific columns in a row
 * store.fetch("database", test, "row-key")
 *      .columns("column1", "column2")
 *      .build().subscribe(observer);
 *
 * // Fetch last 5 columns in a row (useful with ordered wide rows)
 * store.fetch("database", test, "row-key")
 *      .reverse(true)
 *      .limit(5)
 *      .build().subscribe(observer);
 *
 * // Query rows based on secondary indexes
 * store.query("database", test)
 *      .where("column1", "value1")
 *      .where("column3", 1000, Operator.LT)
 *      .build().subscribe(observer);
 *
 * </pre>
 */
public interface StoreService {

	/**
	 * Check if the named database exists.
	 */
	boolean has(String database) throws Exception;

	/**
	 * Create a new database.
	 */
	void create(String database) throws Exception;

	/**
	 * Delete the named database and all data in it.
	 */
	void delete(String database) throws Exception;

	/**
	 * Check if the specified table exists.
	 */
	<R extends Comparable<R>, C extends Comparable<C>, V> boolean has(String database, Table<R, C, V> table)
			throws Exception;

	/**
	 * Create the specified table with dynamic columns (wide rows).
	 */
	<R extends Comparable<R>, C extends Comparable<C>, V> void create(String database, Table<R, C, V> table)
			throws Exception;

	/**
	 * Update an existing table definition with dynamic columns (wide rows).
	 */
	<R extends Comparable<R>, C extends Comparable<C>, V> void update(String database, Table<R, C, V> table)
			throws Exception;

	/**
	 * Delete all data in the specified table, but do not remove the table.
	 */
	<R extends Comparable<R>, C extends Comparable<C>, V> void truncate(String database, Table<R, C, V> table)
			throws Exception;

	/**
	 * Delete the specified table and all data in it.
	 */
	<R extends Comparable<R>, C extends Comparable<C>, V> void delete(String database, Table<R, C, V> table)
			throws Exception;

	/**
	 * Start a batch update process for the specified database. The Batch object
	 * supports a fluent API for batching one or multiple row updates together
	 * into one query execution.
	 */
	Batch batch(String database) throws Exception;

	/**
	 * Check if the specified row key exists in a table.
	 */
	<R extends Comparable<R>, C extends Comparable<C>, V> Observable<Boolean> exists(String database,
			Table<R, C, V> table, R key) throws Exception;

	/**
	 * Fetch a set of rows by row key.
	 */
	<R extends Comparable<R>, C extends Comparable<C>, V> ObservableQueryBuilder<R, C> fetch(String database,
			Table<R, C, V> table, R... keys) throws Exception;

	/**
	 * Query for rows from a secondary column value index. There must be at
	 * least one .where() clause used with this builder.
	 */
	<R extends Comparable<R>, C extends Comparable<C>, V> ObservableIndexQueryBuilder<R, C> query(String database,
			Table<R, C, V> table) throws Exception;

}
