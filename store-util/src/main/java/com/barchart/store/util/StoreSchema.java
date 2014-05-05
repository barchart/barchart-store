package com.barchart.store.util;

import com.barchart.store.api.StoreService;
import com.barchart.store.api.Table;

/**
 * Base class for defining store schemas.
 */
public abstract class StoreSchema {

	/**
	 * Override in subclass to provide table definitions for the update process.
	 */
	protected abstract Table<?, ?, ?>[] tables();

	/**
	 * Compare the schema with the remote database and update if needed. Not
	 * recommended for unattended running on production data sets.
	 */
	public void update(final StoreService store, final String database)
			throws Exception {

		if (!store.has(database)) {
			store.create(database);
		}

		final Table<?, ?, ?>[] tables = tables();

		for (final Table<?, ?, ?> table : tables) {
			if (!store.has(database, table)) {
				store.create(database, table);
			} else {
				store.update(database, table);
			}
		}

	}

	/**
	 * Truncate all tables in the current schema to reset the database.
	 */
	public void truncate(final StoreService store, final String database)
			throws Exception {

		if (store.has(database)) {

			final Table<?, ?, ?>[] tables = tables();

			for (final Table<?, ?, ?> table : tables) {
				if (store.has(database, table)) {
					store.truncate(database, table);
				}
			}

		}

	}

}
