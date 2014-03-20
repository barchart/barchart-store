package com.barchart.store.heap;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.barchart.store.api.StoreColumn;
import com.barchart.store.api.StoreRow;

public class HeapRow<R extends Comparable<R>, C extends Comparable<C>> implements StoreRow<R, C> {

	private final R key;
	private final ConcurrentNavigableMap<C, HeapColumn<C>> columns;

	public HeapRow(final R key_) {
		key = key_;
		columns = new ConcurrentSkipListMap<C, HeapColumn<C>>();
	}

	public HeapRow(final R key_, final Comparator<C> comparator_) {
		key = key_;
		columns = new ConcurrentSkipListMap<C, HeapColumn<C>>(comparator_);
	}

	@Override
	public R getKey() {
		return key;
	}

	protected NavigableSet<C> unsafeColumns() {
		return columns.keySet();
	}

	@Override
	public Collection<C> columns() {
		return Collections.unmodifiableCollection(columns.keySet());
	}

	@Override
	public StoreColumn<C> getByIndex(int index) {

		final Iterator<C> iter = columns.keySet().iterator();

		while (index > 0) {
			if (!iter.hasNext()) {
				throw new ArrayIndexOutOfBoundsException();
			}
			iter.next();
			index--;
		}

		if (!iter.hasNext()) {
			throw new ArrayIndexOutOfBoundsException();
		}

		return columns.get(iter.next());

	}

	@Override
	public StoreColumn<C> get(final C name) {
		return columns.get(name);
	}

	protected HeapColumn<C> getImpl(final C name) {
		return columns.get(name);
	}

	protected void update(final HeapColumn<C> column) {
		columns.put(column.getName(), column);
	}

	protected void delete(final C name) {
		columns.remove(name);
	}

	@Override
	public boolean equals(final Object that) {
		if (that instanceof HeapRow) {
			return key.equals(((HeapRow<?, ?>) that).key);
		}
		return false;
	}

	@Override
	public int compareTo(final StoreRow<R, C> o) {
		return key.compareTo(o.getKey());
	}

}
