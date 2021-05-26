package edu.brown.cs.zkbenchmark;

import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.apache.log4j.Logger;

import edu.brown.cs.zkbenchmark.ZooKeeperBenchmark.TestType;

public class SyncBenchmarkClient extends BenchmarkClient {

	AtomicInteger _totalOps;
	private boolean _syncfin;

	private static final Logger LOG = Logger.getLogger(SyncBenchmarkClient.class);

	public SyncBenchmarkClient(ZooKeeperBenchmark zkBenchmark, String host, String namespace, int attempts, int id) throws IOException {
		super(zkBenchmark, host, namespace, attempts, id);
	}

	@Override
	protected void submit(int n, TestType type) {
		try {
			submitWrapped(n, type);
		} catch (Exception e) {
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
				case READ:
					_client.getData().forPath(_path);
					break;

				case CREATE:
					data = new String(_zkBenchmark.getData() + i).getBytes();
					_client.create().forPath(_path + "/" + _count, data);
					break;

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

				case AR:
					this.setTest(TestType.ACQUIRE);
					String path = "/lock";
					while (true) {
						try {
							_client.create().withMode(CreateMode.EPHEMERAL).forPath(path, new byte[0]);
							LOG.info("-- Client #" + _id + " acquires the lock");
							break;
						} catch (NodeExistsException e) {
							_async.watched().checkExists().forPath(path).event().thenAccept(event -> {
								// System.out.println("-- Client #" + _id + " wakes up");
								synchronized (_lock) {
									_lock.notify();
								}
								// System.out.println("-- Client #" + _id + " tries again");
							});
							LOG.info("-- Client #" + _id + " fails to acquire the lock, wait to be waked up by the watcher callback");
							synchronized (_lock) {
								_lock.wait();
							}
						}
					}

					this.setTest(TestType.RELEASE);
					while (true)
						try {
							// ToDo need to check exists first
							_client.delete().forPath(path);
							LOG.info("-- Client #" + _id + " releases the lock");
							break;
						} catch (Exception e) {
							LOG.info("-- Client #" + _id + " fails to release the lock");
						}
					this.setTest(TestType.AR);
					break;
				default:
					LOG.error("Unknown Test Type");

			}

			recordElapsedInterval(new Double(submitTime));
			_count++; 							
			_zkBenchmark.incrementFinished();

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
	**/
	@Override
	protected void resubmit(int n) {
		_totalOps.getAndAdd(n);
	}
}
