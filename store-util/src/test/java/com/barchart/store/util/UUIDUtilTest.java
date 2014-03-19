package com.barchart.store.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.UUID;

import org.junit.Test;

public class UUIDUtilTest {

	TimeUUIDComparator cmp = new TimeUUIDComparator();

	@Test
	public void testTimestamp() throws Exception {

		final long millis = System.currentTimeMillis();
		final UUID gen = UUIDUtil.timeUUID(millis);

		assertEquals(millis, UUIDUtil.timestampFrom(gen));

	}

	@Test
	public void testSequence() throws Exception {

		final UUID[] uuids = new UUID[16384];

		// Max IDs per node is 16384/ms. Verify that there's no sequence overlap
		final long millis = System.currentTimeMillis();
		for (int i = 0; i < uuids.length; i++) {
			uuids[i] = UUIDUtil.timeUUID(millis);
		}
		for (int i = 0; i < uuids.length; i++) {
			for (int j = i + 1; j < uuids.length; j++) {
				assertTrue(uuids[i].clockSequence() != uuids[j].clockSequence());
			}
		}

	}

	@Test(expected = AssertionError.class)
	public void testSequenceOverrun() throws Exception {

		final UUID[] uuids = new UUID[16385];

		// Max IDs per node is 16384/ms. Doing 16385 should produce an assertion
		// error
		final long millis = System.currentTimeMillis();
		for (int i = 0; i < uuids.length; i++) {
			uuids[i] = UUIDUtil.timeUUID(millis);
		}
		for (int i = 0; i < uuids.length; i++) {
			for (int j = i + 1; j < uuids.length; j++) {
				assertTrue(uuids[i].clockSequence() != uuids[j].clockSequence());
			}
		}

	}

	@Test
	public void testSingleMinMax() throws Exception {

		final UUID gen = UUIDUtil.timeUUID(1000);
		verifyUUID(gen);
		final long timestamp = gen.timestamp();

		final UUID min = UUIDUtil.timeUUIDMin(1000);
		verifyUUID(min);
		assertEquals(timestamp, min.timestamp());

		final UUID max = UUIDUtil.timeUUIDMax(1000);
		verifyUUID(max);
		assertEquals(timestamp, max.timestamp());

		assertTrue(cmp.compare(min, gen) <= 0);
		assertTrue(cmp.compare(min, max) <= 0);
		assertTrue(cmp.compare(gen, max) <= 0);

	}

	@Test
	public void testMinMax() throws Exception {

		// Run this a whole bunch to guard against randomness errors
		for (int i = 0; i < 5000000; i++) {

			final long millis = (long) Math.floor(Math.random() * System.currentTimeMillis());
			final long lower = (long) Math.floor(Math.random() * millis);
			final long upper = (long) Math.floor(Math.random() * (System.currentTimeMillis() - millis)) + millis;

			final UUID gen = UUIDUtil.timeUUID(millis);
			final UUID min = UUIDUtil.timeUUIDMin(lower);
			final UUID max = UUIDUtil.timeUUIDMax(upper);

			verifyUUID(gen);
			verifyUUID(min);
			verifyUUID(max);

			try {
				assertTrue(cmp.compare(min, gen) <= 0);
				assertTrue(cmp.compare(min, max) <= 0);
				assertTrue(cmp.compare(gen, max) <= 0);
			} catch (final AssertionError t) {
				System.out.println("FAILED (" + i + "):\n");
				System.out.print("min: ");
				debugUUID(min);
				System.out.print("\nmid: ");
				debugUUID(gen);
				System.out.print("\nmax: ");
				debugUUID(max);
				throw t;
			}

		}

	}

	private void debugUUID(final UUID uuid) {

		System.out.println(uuid);
		System.out.println("most:  " + uuid.getMostSignificantBits());
		System.out.println("least: " + uuid.getLeastSignificantBits());
		System.out.println("time:  " + uuid.timestamp());
		System.out.println("node:  " + uuid.node());
		System.out.println("clock: " + uuid.clockSequence());
		System.out.println("ver:   " + uuid.version());
		System.out.println("var:   " + uuid.variant());

	}

	private void verifyUUID(final UUID uuid) {

		assertEquals(1, uuid.version());
		assertEquals(2, uuid.variant());

	}

}
