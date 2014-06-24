package com.barchart.store.cassandra;

import static org.junit.Assert.*;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.barchart.store.api.Table;
import com.barchart.util.test.concurrent.CallableTest;
import com.netflix.astyanax.model.ConsistencyLevel;

@Ignore
public class TestCassandraLockProvider {

	private static CassandraStore store1;
	private static CassandraStore store2;
	private static Table<String, UUID, Long> locks = Table.builder("locks")
			.rowKey(String.class)
			.columnKey(UUID.class)
			.defaultType(Long.class)
			.build();

	private CassandraLockProvider lockService1;
	private CassandraLockProvider lockService2;

	@Test
	public void testLocalUncontestedLock() throws Exception {
		final Lock testLock = lockService1.localLock("test");
		assertTrue(testLock.tryLock());
		testLock.unlock();
	}

	@Test
	public void testLocalReLock() throws Exception {
		final Lock testLock = lockService1.localLock("test");
		testLock.lock();
		assertTrue(testLock.tryLock());
		testLock.unlock();
	}

	@Test
	public void testLocalMultiLock() throws Exception {
		final Lock lock1 = lockService1.localLock("test");
		final Lock lock2 = lockService1.localLock("test2");
		assertTrue(lock1.tryLock());
		assertTrue(lock2.tryLock());
		lock1.unlock();
		lock2.unlock();
	}

	@Test
	public void testLocalContestedLock() throws Exception {
		final Lock lock1 = lockService1.localLock("test");
		final Lock lock2 = lockService1.localLock("test");
		lock1.lock();
		assertFalse(lock2.tryLock());
		lock1.unlock();
		assertTrue(lock2.tryLock());
		lock2.unlock();
	}

	// Hard to test non-global locks; most of the time "local" behaves like global if nodes are running smoothly
	// @Test
	public void testSeparateLocalLock() throws Exception {
		final Lock lock1 = lockService1.localLock("test");
		final Lock lock2 = lockService2.localLock("test");
		assertTrue(lock1.tryLock());
		assertTrue(lock2.tryLock());
		lock1.unlock();
		lock2.unlock();
	}

	@Test
	public void testGlobalLock() throws Exception {
		final Lock lock1 = lockService1.globalLock("test");
		final Lock lock2 = lockService2.globalLock("test");
		lock1.lock();
		assertFalse(lock2.tryLock());
		lock1.unlock();
		assertTrue(lock2.tryLock());
		lock2.unlock();
	}

	@Test
	public void testLockWait() throws Exception {

		final Lock lock1 = lockService1.localLock("test");
		lock1.lock();

		final Lock lock2 = lockService1.localLock("test");

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
		lockService1 = new CassandraLockProvider(store1, "locks", locks);
		lockService2 = new CassandraLockProvider(store2, "locks", locks);
	}

	@BeforeClass
	public static void init() throws Exception {

		final Properties props = new Properties();
		props.load(TestCassandraStore.class
				.getResourceAsStream("/cassandra.store"));

		final String[] seeds = props.get("seeds").toString().split(",");
		final String username = props.get("username").toString();
		final String password = props.get("password").toString();

		final String dc1 = props.get("dc1").toString();
		final String dc2 = props.get("dc1").toString();

		store1 = new CassandraStore();
		store1.setZones(dc1, dc2);
		store1.setReplicationFactor(2);
		store1.setSeeds(seeds);
		store1.setLocalZone(dc1);
		store1.setCredentials(username, password);
		store1.setReadConsistency(ConsistencyLevel.CL_ALL);
		store1.setWriteConsistency(ConsistencyLevel.CL_ALL);
		store1.connect();

		store2 = new CassandraStore();
		store2.setZones(dc1, dc2);
		store2.setReplicationFactor(2);
		store2.setSeeds(seeds);
		store2.setLocalZone(dc2);
		store2.setCredentials(username, password);
		store2.setReadConsistency(ConsistencyLevel.CL_ALL);
		store2.setWriteConsistency(ConsistencyLevel.CL_ALL);
		store2.connect();

		if (!store1.has("locks")) {
			store1.create("locks");
			CallableTest.waitFor(new Callable<Boolean>() {
				@Override
				public Boolean call() throws Exception {
					return store1.has("locks");
				}
			}, 5000);
			CallableTest.waitFor(new Callable<Boolean>() {
				@Override
				public Boolean call() throws Exception {
					return store2.has("locks");
				}
			}, 5000);
		}

		if (!store1.has("locks", locks)) {
			store1.create("locks", locks);
			CallableTest.waitFor(new Callable<Boolean>() {
				@Override
				public Boolean call() throws Exception {
					return store1.has("locks", locks);
				}
			}, 5000);
			CallableTest.waitFor(new Callable<Boolean>() {
				@Override
				public Boolean call() throws Exception {
					return store2.has("locks", locks);
				}
			}, 5000);
		}

	}

}
