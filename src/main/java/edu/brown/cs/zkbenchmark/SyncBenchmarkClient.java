package edu.brown.cs.zkbenchmark;

import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletionStage;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.apache.log4j.Logger;

import org.apache.curator.framework.api.CuratorEventType;

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
		byte data[];

		for (int i = 0; true; i++) {
			double submitTime = ((double) System.nanoTime() - _zkBenchmark.getStartTime()) / 1000000000.0;

			switch (type) {
				case UNDEFINED:
					LOG.error("Test type was UNDEFINED. No tests executed");
					break;

				case READ:
					_client.getData().forPath(_path);
					break;

				case AR:
					// this.setTest(TestType.ACQUIRE);
					while (true) {
						try {
							_client.create().withMode(CreateMode.EPHEMERAL).forPath(_lockPath, new byte[0]);
							Stat stat = _client.checkExists().forPath(_lockPath);
							if (stat == null) {
								continue;
							}
							LOG.info("Client #" + _id + " acquires the lock");
							break;
						} catch (NodeExistsException e) {
							_async.watched().checkExists().forPath(_lockPath).event().thenAccept(event -> {
								if (true) {
									LOG.info("Client #" + _id + " wakes up by " + event.getType());
									synchronized (_lock) {
										_lock.notify();
									}
								}

								((CompletionStage<WatchedEvent>) event).exceptionally(exception -> {
									LOG.warn("Client #" + _id + " should not happen");
									return null;
								});

							});

							LOG.info("Client #" + _id + " fails to acquire the lock, wait to be waked up by the watcher callback");
							synchronized (_lock) {
								// _lock.wait();
								_lock.wait((long) 100);
							}
						}
					}

					while (true)
						try {
							// // ToDo need to check exists first
							// Stat stat = _client.checkExists().forPath(_lockPath);
							// if (stat != null) {
							// 	_client.delete().forPath(_lockPath);
							// 	LOG.info("Client #" + _id + " releases the lock");
							// 	break;
							// } else {
							// 	LOG.warn("Client #" + _id + " fails to release the lock");
							// }
							_client.delete().forPath(_lockPath);
							LOG.info("Client #" + _id + " releases the lock");
							break;
						} catch (Exception e) {
							LOG.info("Client #" + _id + " fails to release the lock");
						}
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
				if (type == TestType.AR) {
					try {
						_client.create().withMode(CreateMode.EPHEMERAL).forPath(_lockPath, new byte[0]);
						// sleep for 10ms, wait for the other clients to start the watcher
						Thread.sleep(10);
						_client.delete().forPath(_lockPath);
					} catch (Exception e) {
						// do nothing
					}
				}
				break;
			}
		}
	}

	@Override
	protected void finish() {
		_syncfin = true;
		try {
			_lock.notify();
		} catch (Exception e) {
			// do nothing
		}
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
