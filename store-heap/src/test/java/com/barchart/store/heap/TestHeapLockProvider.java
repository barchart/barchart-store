package com.barchart.store.heap;

import static org.junit.Assert.*;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.junit.Before;
import org.junit.Test;

import com.barchart.store.api.Table;

public class TestHeapLockProvider {

	private HeapStore store;
	private HeapLockProvider lockService;

	@Test
	public void testUncontestedLock() throws Exception {
		final Lock testLock = lockService.localLock("test");
		assertTrue(testLock.tryLock());
		testLock.unlock();
	}

	@Test
	public void testReLock() throws Exception {
		final Lock testLock = lockService.localLock("test");
		testLock.lock();
		assertTrue(testLock.tryLock());
		testLock.unlock();
	}

	@Test
	public void testMultiLock() throws Exception {
		final Lock lock1 = lockService.localLock("test");
		assertTrue(lock1.tryLock());
		final Lock lock2 = lockService.localLock("test2");
		assertTrue(lock2.tryLock());
		lock1.unlock();
		lock2.unlock();
	}

	@Test
	public void testContestedLock() throws Exception {
		final Lock lock1 = lockService.localLock("test");
		lock1.lock();
		final Lock lock2 = lockService.localLock("test");
		assertFalse(lock2.tryLock());
		lock1.unlock();
		assertTrue(lock2.tryLock());
		lock2.unlock();
	}

	@Test
	public void testLockWait() throws Exception {

		final Lock lock1 = lockService.localLock("test");
		lock1.lock();

		final Lock lock2 = lockService.localLock("test");

		new Thread() {
			@Override
			public void run() {
				try {
					Thread.sleep(500);
				} catch (final InterruptedException e) {
				} finally {
					lock1.unlock();
				}
			}
		}.start();

		assertTrue(lock2.tryLock(1, TimeUnit.SECONDS));

		lock2.unlock();

	}

	@Before
	public void setUp() throws Exception {

		store = new HeapStore();

		store.create("locks");

		final Table<String, UUID, Long> locks = Table.builder("locks")
				.rowKey(String.class)
				.columnKey(UUID.class)
				.defaultType(Long.class)
				.build();

		store.create("locks", locks);

		lockService = new HeapLockProvider(store, "locks", locks);

	}
}
