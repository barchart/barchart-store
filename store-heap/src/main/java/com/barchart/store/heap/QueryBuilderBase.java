package com.barchart.store.heap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

import com.barchart.store.api.ObservableQueryBuilder;
import com.barchart.store.api.StoreColumn;
import com.barchart.store.api.StoreRow;

public abstract class QueryBuilderBase<R extends Comparable<R>, C extends Comparable<C>> implements
		ObservableQueryBuilder<R, C> {

	protected Collection<C> columns = null;
	protected C start = null;
	protected C end = null;
	protected int first = 0;
	protected int last = 0;
	protected String prefix = null;

	@Override
	public ObservableQueryBuilder<R, C> first(final int limit) {
		first = limit;
		return this;
	}

	@Override
	public ObservableQueryBuilder<R, C> last(final int limit) {
		last = limit;
		return this;
	}

	@Override
	public ObservableQueryBuilder<R, C> start(final C column) {
		start = column;
		return this;
	}

	@Override
	public ObservableQueryBuilder<R, C> end(final C column) {
		end = column;
		return this;
	}

	@Override
	public ObservableQueryBuilder<R, C> prefix(final String prefix_) {
		prefix = prefix_;
		return this;
	}

	@Override
	public ObservableQueryBuilder<R, C> columns(final C... columns_) {
		columns = new ArrayList<C>(Arrays.asList(columns_));
		return this;
	}

	protected class RowFilter implements StoreRow<R, C> {

		private final HeapRow<R, C> row;
		private final List<C> availColumns;

		public RowFilter(final HeapRow<R, C> row_) {
			row = row_;
			availColumns = new ArrayList<C>();
			buildColumnList();
		}

		private void buildColumnList() {

			if (first > 0) {

				int index = 0;
				final Iterator<C> iter = row.columns().iterator();

				while (iter.hasNext() && index < first) {
					availColumns.add(iter.next());
					index++;
				}

			} else if (last > 0) {

				int index = 0;
				final int offset = row.columns().size() - last;
				final Iterator<C> iter = row.columns().iterator();

				while (iter.hasNext()) {
					if (index < offset) {
						index++;
						iter.next();
						continue;
					}
					availColumns.add(iter.next());
				}

			} else if (start != null || end != null) {

				final SortedSet<C> rc = row.columnsImpl();
				if (start == null) {
					start = rc.first();
				} else if (end == null) {
					end = rc.last();
				}

				availColumns.addAll(rc.subSet(start, end));
				// subSet is tail-exclusive
				if (rc.contains(end)) {
					availColumns.add(end);
				}

			} else if (prefix != null) {

				final Iterator<C> iter = row.columns().iterator();

				while (iter.hasNext()) {
					final C name = iter.next();
					if (name.toString().startsWith(prefix)) {
						availColumns.add(name);
					}
				}

			} else if (columns != null) {

				columns.retainAll(row.columns());
				availColumns.addAll(columns);

			} else {

				availColumns.addAll(row.columns());

			}

		}

		@Override
		public R getKey() {
			return row.getKey();
		}

		@Override
		public Collection<C> columns() {
			return Collections.unmodifiableCollection(availColumns);
		}

		@Override
		public StoreColumn<C> getByIndex(final int index) {

			if (availColumns.size() > 0) {

				if (index > availColumns.size() - 1) {
					throw new ArrayIndexOutOfBoundsException();
				}

				return row.get(availColumns.get(index));

			} else {
				return row.getByIndex(index);
			}

		}

		@Override
		public StoreColumn<C> get(final C name) {
			if (availColumns.size() == 0 || availColumns.contains(name)) {
				return row.get(name);
			}
			return null;
		}

		@Override
		public int compareTo(final StoreRow<R, C> o) {
			return getKey().compareTo(o.getKey());
		}

	}

}
