package com.barchart.store.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.Enumeration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public final class UUIDUtil {

	/**
	 * Minimum possible value for node ID and clock sequences for sorting /
	 * range queries.
	 */
	private final static long MIN_SEQUENCE_AND_NODE = 0x8000000000000000L;

	/**
	 * Maximum possible value for node ID and clock sequences for sorting /
	 * range queries.
	 */
	private final static long MAX_SEQUENCE_AND_NODE = 0xBFFFFFFFFFFFFFFFL;

	// This comes from Hector's TimeUUIDUtils class:
	// https://github.com/rantav/hector/blob/master/core/src/main/java/me/...
	private static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

	/**
	 * Local node/sequence mask.
	 */
	private static long clockSeqAndNode = MIN_SEQUENCE_AND_NODE | NodeUtil.node();

	/**
	 * Current local sequence number.
	 */
	private static AtomicLong sequence = new AtomicLong((long) (Math.random() * 0x3FFF));

	private UUIDUtil() {
	}

	/**
	 * Creates a new UUID using current system time and clock sequence.
	 */
	public static final UUID timeUUID() {
		return new UUID(newTime(), getClockSeqAndNode());
	}

	/**
	 * Creates a new UUID using provided time and current clock sequence.
	 */
	public static final UUID timeUUID(final long time) {
		return new UUID(createTime(time), getClockSeqAndNode());
	}

	/**
	 * Creates a new UUID using provided time and lowest possible clock
	 * sequence.
	 */
	public static final UUID timeUUIDMin(final long time) {
		return new UUID(createTime(time), MIN_SEQUENCE_AND_NODE);
	}

	/**
	 * Creates a new UUID using provided time and largest possible clock
	 * sequence.
	 */
	public static final UUID timeUUIDMax(final long time) {
		return new UUID(createTime(time), MAX_SEQUENCE_AND_NODE);
	}

	/**
	 * Parse an epoch timestamp from the UUID's embedded timestamp.
	 */
	public static final long timestampFrom(final UUID uuid) {
		return (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
	}

	/**
	 * Create a new UUID timestamp from the current system time.
	 */
	private static long newTime() {
		return createTime(System.currentTimeMillis());
	}

	/**
	 * Creates a new time field from the given epoch timestamp.
	 */
	static long createTime(final long timestamp) {

		final long timeMillis = (timestamp * 10000) + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH;

		return timeMillis << 32 // time low
				| ((timeMillis >> 16) & 0xFFFF0000) // time mid
				| ((timeMillis >> 48) & 0x0FFF) // time hi
				| 0x1000; // version 1

	}

	/**
	 * Increments the sequence and returns the new node/sequence value for the
	 * next UUID.
	 */
	private static long getClockSeqAndNode() {
		return clockSeqAndNode | ((sequence.getAndIncrement() & 0x3FFF) << 48);
	}

	/*
	 * Local node identity / initialization
	 */

	private static class NodeUtil {

		private static final SecureRandom random = new SecureRandom();

		private NodeUtil() {
		}

		public static long node() {

			long node = fromJava();

			if (node == 0) {
				node = fromSystem();
			}

			if (node == 0) {
				node = fromInet();
			}

			if (node == 0) {
				node = fromRandom();
			}

			return node;

		}

		/**
		 * Generate node ID by looking up MAC address from JVM.
		 */
		private static long fromJava() {

			long addr = 0;

			try {
				final Enumeration<NetworkInterface> ifs = NetworkInterface.getNetworkInterfaces();
				if (ifs != null) {
					while (ifs.hasMoreElements()) {
						final NetworkInterface iface = ifs.nextElement();
						final byte[] hw = iface.getHardwareAddress();
						if (hw != null && hw.length == 6 && hw[1] != (byte) 0xff) {
							for (int i = 0; i < hw.length; i++) {
								addr = (addr << 8) | (hw[i] & 0xFFl);
							}
							break;
						}
					}
				}
			} catch (final SocketException ex) {
				// Ignore it.
			}

			return addr;

		}

		/**
		 * Generate node ID by looking up MAC address from local OS.
		 */
		private static long fromSystem() {

			Process p = null;
			BufferedReader in = null;

			try {

				final String osname = System.getProperty("os.name", "");

				if (osname.startsWith("Windows")) {
					p = openProcess("ipconfig", "/all");
				} else if (osname.startsWith("Solaris") || osname.startsWith("SunOS")) {
					// Solaris code must appear before the generic code
					final String hostName = getFirstLineOfCommand(openProcess("uname", "-n"));
					if (hostName != null) {
						p = openProcess("/usr/sbin/arp", hostName);
					}
				} else if (new File("/usr/sbin/lanscan").exists()) {
					p = openProcess("/usr/sbin/lanscan");
				} else if (new File("/sbin/ifconfig").exists()) {
					p = openProcess("/sbin/ifconfig", "-a");
				}

				if (p != null) {
					in = new BufferedReader(new InputStreamReader(p.getInputStream()), 128);
					String l = null;
					while ((l = in.readLine()) != null) {
						final long node = parseForMacAddress(l);
						if (node != 0) {
							return node;
						}
					}
				}

			} catch (final Throwable t) {
				t.printStackTrace();

			} finally {

				if (in != null) {
					try {
						in.close();
					} catch (final IOException ex) {
						// Ignore it.
					}
				}

				closeProcess(p);

			}

			return 0;

		}

		/**
		 * Generate node ID from local IP address.
		 */
		private static long fromInet() {

			try {
				long node = 0;
				final byte[] local = InetAddress.getLocalHost().getAddress();
				node |= (local[0] << 24) & 0xFF000000L;
				node |= (local[1] << 16) & 0xFF0000;
				node |= (local[2] << 8) & 0xFF00;
				node |= local[3] & 0xFF;
				return node;
			} catch (final UnknownHostException ex) {
			}

			return 0;

		}

		/**
		 * Random node ID.
		 */
		private static long fromRandom() {
			return (long) (random.nextDouble() * 0x7FFFFFFF);
		}

		/**
		 * Open a local shell process.
		 */
		private static Process openProcess(final String... commands) throws IOException {
			return Runtime.getRuntime().exec(commands, null);
		}

		/**
		 * Close/cleanup a local shell process.
		 */
		private static void closeProcess(final Process process) {

			if (process != null) {

				try {
					process.getErrorStream().close();
				} catch (final IOException ex) {
					// Ignore it.
				}

				try {
					process.getOutputStream().close();
				} catch (final IOException ex) {
					// Ignore it.
				}

				process.destroy();

			}

		}

		/**
		 * Returns the first output line of the given shell command.
		 *
		 * @param commands The command/arguments to run
		 */
		private static String getFirstLineOfCommand(final Process p) throws IOException {

			BufferedReader reader = null;

			try {

				reader = new BufferedReader(new InputStreamReader(p.getInputStream()), 128);
				return reader.readLine();

			} finally {

				if (reader != null) {
					try {
						reader.close();
					} catch (final IOException ex) {
						// Ignore it.
					}
				}

				closeProcess(p);

			}

		}

		/**
		 * Attempts to find a pattern in the given String.
		 *
		 * @param in the String, may not be <code>null</code>
		 * @return the substring that matches this pattern or <code>null</code>
		 */
		private static long parseForMacAddress(final String in) {

			String out = in;

			// lanscan

			final int hexStart = out.indexOf("0x");

			if (hexStart != -1 && out.indexOf("ETHER") != -1) {

				final int hexEnd = out.indexOf(' ', hexStart);

				if (hexEnd > hexStart + 2) {
					out = out.substring(hexStart, hexEnd);
				}

			} else {

				int octets = 0;
				int lastIndex, old, end;

				if (out.indexOf('-') > -1) {
					out = out.replace('-', ':');
				}

				lastIndex = out.lastIndexOf(':');

				if (lastIndex > out.length() - 2) {
					out = null;
				} else {

					end = Math.min(out.length(), lastIndex + 3);
					++octets;
					old = lastIndex;
					while (octets != 5 && lastIndex != -1 && lastIndex > 1) {
						lastIndex = out.lastIndexOf(':', --lastIndex);
						if (old - lastIndex == 3 || old - lastIndex == 2) {
							++octets;
							old = lastIndex;
						}
					}

					if (octets == 5 && lastIndex > 1) {
						out = out.substring(lastIndex - 2, end).trim();
					} else {
						out = null;
					}

				}

			}

			if (out != null && out.startsWith("0x")) {
				out = out.substring(2);
			}

			if (out != null && parseHex(out, 4) != 0xff) {
				return parseHex(out, 16);
			}

			return 0;

		}

		/**
		 * Parses a <code>long</code> from a hex encoded number. This method
		 * will skip all characters that are not 0-9, A-F and a-f.
		 *
		 * Returns 0 if the {@link CharSequence} does not contain any
		 * interesting characters.
		 */
		private static long parseHex(final CharSequence s, final int octets) {

			long out = 0;
			byte quartets = 0;
			char c;
			byte b;

			for (int i = 0; i < s.length() && quartets < octets * 2; i++) {

				c = s.charAt(i);

				if ((c > 47) && (c < 58)) {
					b = (byte) (c - 48);
				} else if ((c > 64) && (c < 71)) {
					b = (byte) (c - 55);
				} else if ((c > 96) && (c < 103)) {
					b = (byte) (c - 87);
				} else {
					continue;
				}

				out = (out << 4) | b;
				quartets++;

			}

			return out;

		}

	}

}
