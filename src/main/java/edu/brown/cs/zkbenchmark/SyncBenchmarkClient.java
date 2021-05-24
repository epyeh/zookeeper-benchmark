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
		byte data[];

		for (int i = 0; i < _totalOps.get(); i++) {
			double submitTime = ((double) System.nanoTime() - _zkBenchmark.getStartTime()) / 1000000000.0;

			switch (type) {
				// Read 1 znode
				case READ:
					_client.getData().forPath(_path);
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

				case UNDEFINED:
					LOG.error("Test type was UNDEFINED. No tests executed");
					break;
				default:
					LOG.error("Unknown Test Type.");

			}

			recordElapsedInterval(new Double(submitTime));
			_count++; 							// increment _count to keep track of how many operations completed
			_zkBenchmark.incrementFinished(); 	// increment the # of operations that have been completed
												// This is the aggregate global var across all threads
												// to keep track of the total # of operations 
												// ALL the threads together have been able to complete

			// _syncfin will be set to true when finish() is called.
			// This is called in BenchmarkClient under FinishTimer.
			// Finish Timer cancels the timer and then tells the sync client to stop issuing requests by breaking out
			if (_syncfin) {
				break;
			}
		}
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
