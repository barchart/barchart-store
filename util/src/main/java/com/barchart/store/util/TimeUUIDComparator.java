package com.barchart.store.util;

import java.util.Comparator;
import java.util.UUID;

/**
 * Replacement for native UUID.compareTo() because simple long comparisons don't
 * take signs into account and messes up timestamp ordering.
 */
public class TimeUUIDComparator implements Comparator<UUID> {

	@Override
	public int compare(final UUID u1, final UUID u2) {

		final long t1 = u1.timestamp();
		final long t2 = u2.timestamp();

		if (t1 != t2)
			return t1 < t2 ? -1 : 1;

		final long s1 = u1.clockSequence();
		final long s2 = u2.clockSequence();

		return s1 < s2 ? -1 : (s1 == s2 ? 0 : 1);

	}

}
