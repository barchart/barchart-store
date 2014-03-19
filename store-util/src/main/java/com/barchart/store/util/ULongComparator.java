package com.barchart.store.util;

import java.util.Comparator;

/**
 * Unsigned long comparator.
 */
public class ULongComparator implements Comparator<Long> {

	/*
	 * Inspired by:
	 *
	 * http://www.drmaciver.com/2008/08/unsigned-comparison-in-javascala/
	 *
	 * http://www.ragestorm.net/blogs/?p=282
	 */
	@Override
	public int compare(final Long first, final Long second) {

		// Unbox
		final long l1 = first.longValue();
		final long l2 = second.longValue();

		// If both numbers have the same sign, compare normally
		if (!((l1 < 0) ^ (l2 < 0))) {
			return l1 < l2 ? -1 : (l1 == l2 ? 0 : 1);
		}

		// Different signs, if l1 has the MSB set, it’s negative, thus bigger
		if (l1 < 0)
			return 1;

		// Else l2 is bigger
		return -1;

	}

}
