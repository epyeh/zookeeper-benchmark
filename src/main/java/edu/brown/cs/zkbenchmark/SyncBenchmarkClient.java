package edu.brown.cs.zkbenchmark;

import java.io.File;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Random;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import edu.brown.cs.zkbenchmark.ZooKeeperBenchmark.TestType;

public class SyncBenchmarkClient extends BenchmarkClient {

	AtomicInteger _totalOps;
	private boolean _syncfin;

	private static final Logger LOG = Logger.getLogger(SyncBenchmarkClient.class);

	public SyncBenchmarkClient(ZooKeeperBenchmark zkBenchmark, String host, String namespace, int attempts, int id)
			throws IOException {
		super(zkBenchmark, host, namespace, attempts, id); // Initialize variables
	}

	@Override
	protected void submit(int n, TestType type) {
		try {
			submitWrapped(n, type);
		} catch (Exception e) {
			// What can you do? for some reason
			// com.netflix.curator.framework.api.Pathable.forPath() throws Exception
			LOG.error("Error while submitting requests", e);
		}
	}

	protected void submitWrapped(int n, TestType type) throws Exception {
		_syncfin = false;
		_totalOps = _zkBenchmark.getCurrentTotalOps();
		LOG.info("* Initial totalOps = " + _totalOps.get());
		// System.out.println("* Initial totalOps = " + _totalOps.get());
		byte data[];

		// Eric: Why do they use _totalOps.get(). Why not just use n? What's the point
		// of passing _attempts from BenchmarkClient? Shouldn't it be n?
		// Every client shouldn't be looping over the total number of operations. it
		// should be the average # of operations they need to complete

		// I would say this is realy a nasty code.
		// As far as I can tell,
		// the _totalOps is the number of outstanding ops
		// and forget about the initial value of _totalOps
		// The stopping signal is _syncfin

		// BufferedWriter readWriteDecisionFile = null;
		// if (type == TestType.MIXREADWRITE) {
		// readWriteDecisionFile = new BufferedWriter(new FileWriter(new File(
		// "results/" + _id + "-" + _type + this._zkBenchmark.getReadPercentage() +
		// "-read-write.dat")));
		// }

		int numToRead = this._zkBenchmark.getNumToRead();
		int numToWrite = 20 - numToRead;

		int currRead = numToRead;
		int currWrite = numToWrite;

		for (int i = 0; i < _totalOps.get(); i++) {
			double submitTime = ((double) System.nanoTime() - _zkBenchmark.getStartTime()) / 1000000000.0;

			switch (type) {
				// Read 1 znode
				case READ:
					_client.getData().forPath(_path);
					break;

				// Set data for 1 znode
				case SETSINGLE:
					data = new String(_zkBenchmark.getData() + i).getBytes();
					_client.setData().forPath(_path, data);
					break;

				// Set data for multiple znodes
				case SETMULTI:
					try {
						data = new String(_zkBenchmark.getData() + i).getBytes();

						// What does _count % _highestN mean?
						// _count represents the # of operations completed
						// _highestN represents the # of nodes that were created
						// They compute _count % _highestN because they want
						// to cycle through all of the nodes they created
						// and try setting to them
						//
						// example:
						// client0
						// znode0, znode1, znode2, znode3, ...
						// multiset will try to write to znode0, znode1, znode2, znode3 ...
						// That's what they mean by multiset

						_client.setData().forPath(_path + "/" + (_count % _highestN), data);
					} catch (NoNodeException e) {
						LOG.warn("No such node when setting data to mutiple nodes. " + "_path = " + _path
								+ ", _count = " + _count + ", _highestN = " + _highestN, e);
					}
					break;

				// Create new node with path _path and _count
				case CREATE:
					data = new String(_zkBenchmark.getData() + i).getBytes();
					_client.create().forPath(_path + "/" + _count, data);
					_highestN++; // for use with SETMULTI
					break;

				// Case for trying to delete nodes
				case DELETE:
					try {
						_client.delete().forPath(_path + "/" + _count);
					} catch (NoNodeException e) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("No such node (" + _path + "/" + _count + ") when deleting nodes", e);
						}
					}
					break;

				// Case for trying to do mixed reads and writes to nodes
				case MIXREADWRITE:
					// int numToRead = this._zkBenchmark.getNumToRead();
					// Random rand = new Random();
					// int randInt = rand.nextInt(100);
					// int readThreshold = this._zkBenchmark.getReadPercentage();

					try {

						if (currRead != 0) {
							_readWriteDecisionFile.write("read\n");
							_client.getData().forPath(_path + "/read");
							currRead--;
						} else {
							_readWriteDecisionFile.write("write\n");
							data = new String(_zkBenchmark.getData() + i).getBytes();
							_client.setData().forPath(_path + "/write", data);
							currWrite--;
						}

						if (currRead == 0 && currWrite == 0) {
							currRead = numToRead;
							currWrite = numToWrite;
						}

						// Perform a read
						// if (randInt < readThreshold) {
						// // For debug
						// readWriteDecisionFile.write("read\n");
						// _client.getData().forPath(_path + "/read");
						// }
						// // Perform a write
						// else {
						// // For debug
						// readWriteDecisionFile.write("write\n");
						// data = new String(_zkBenchmark.getData() + i).getBytes();
						// _client.setData().forPath(_path + "/write", data);
						// }
						// _client.delete().forPath(_path + "/" + _count);
					} catch (Exception e) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Could not write or read due to error:", e);
						}
					}
					break;
				case UNDEFINED:
					LOG.error("Test type was UNDEFINED. No tests executed");
					break;
				default:
					LOG.error("Unknown Test Type.");

			}

			recordElapsedInterval(new Double(submitTime));
			_count++; // increment _count to keep track of how many operations completed
			_zkBenchmark.incrementFinished(); // increment the # of operations that have been completed
												// This is the aggregate global var across all threads
												// to keep track of the total # of operations ALL the threads
												// together have been able to complete

			// _syncfin will be set to true when finish() is called.
			// This is called in BenchmarkClient under FinishTimer.
			// Finish Timer cancels the timer and then tells the sync client to stop issuing
			// requests by breaking out
			if (_syncfin) {
				// try {
				// if (readWriteDecisionFile != null) {
				// readWriteDecisionFile.close();
				// }
				// } catch (IOException e) {
				// LOG.warn("Error while closing readWriteDecision file:", e);
				// }
				break;
			}

		}
		// try {
		// if (readWriteDecisionFile != null) {
		// readWriteDecisionFile.close();
		// }
		// } catch (IOException e) {
		// LOG.warn("Error while closing readWriteDecision file:", e);
		// }

	}

	@Override
	protected void finish() {
		_syncfin = true;
	}

	/**
	 * in fact, n here can be arbitrary number as synchronous operations can be
	 * stopped after finishing any operation.
	 */
	@Override
	protected void resubmit(int n) {
		_totalOps.getAndAdd(n);
	}
}
