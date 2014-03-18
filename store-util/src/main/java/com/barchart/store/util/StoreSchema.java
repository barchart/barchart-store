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

}
