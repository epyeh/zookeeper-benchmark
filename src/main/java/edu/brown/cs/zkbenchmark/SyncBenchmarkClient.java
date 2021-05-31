package edu.brown.cs.zkbenchmark;

import java.lang.Throwable;
import java.time.Duration;
import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.log4j.Logger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.api.BackgroundCallback;

import edu.brown.cs.zkbenchmark.ZooKeeperBenchmark.TestType;

public class SyncBenchmarkClient extends BenchmarkClient {

	AtomicInteger _totalOps;
	private boolean _syncfin;

	private static final Logger LOG = Logger.getLogger(SyncBenchmarkClient.class);

	private static final int TIMEOUT_DURATION = 5; // Represents the timeout duration for a
													// synchronous sync call to return. Units is
													// seconds

	public SyncBenchmarkClient(ZooKeeperBenchmark zkBenchmark, String host, String namespace,
			int attempts, int id) throws IOException {
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

		for (int i = 0; true; i++) {
			double submitTime =
					((double) System.nanoTime() - _zkBenchmark.getStartTime()) / 1000000000.0;

			switch (type) {
				// Read 1 znode
				case READ:
					_client.getData().forPath(_path);
					break;

				// Case for trying to do mixed reads and writes to nodes
				case MIXREADWRITE:

					double randDouble = Math.random();
					double readThreshold = this._zkBenchmark.getReadPercentage();

					if (randDouble < readThreshold) {
						_client.getData().forPath(_path);
					} else {
						data = new String(_zkBenchmark.getData() + i).getBytes();
						_client.setData().forPath(_path, data);
					}
					break;

				case WRITESYNCREAD:

					// First Write Sync
					// Synchrnous Write
					data = new String(_zkBenchmark.getData() + i).getBytes();
					_client.setData().forPath(_path, data);
					_zkBenchmark.incrementFinished();

					// Synchronous Sync
					// 5 seconds for sync to return or we get false
					boolean res = this.synchronousSync(Duration.ofSeconds(TIMEOUT_DURATION)); 
					if (!res) {
						System.out.println("Sync did not return within " + TIMEOUT_DURATION
								+ " seconds. This should not happen!!");
						LOG.error("Sync did not return within " + TIMEOUT_DURATION
								+ " seconds. This should not happen.");
					}

					// Second Write-Sync
					// Synchrnous Write
					data = new String(_zkBenchmark.getData() + i).getBytes();
					_client.setData().forPath(_path, data);
					_zkBenchmark.incrementFinished();

					// Synchronous Sync
					// 5 seconds for sync to return or we get false
					res = this.synchronousSync(Duration.ofSeconds(TIMEOUT_DURATION)); 
					if (!res) {
						System.out.println("Sync did not return within " + TIMEOUT_DURATION
								+ " seconds. This should not happen!!");
						LOG.error("Sync did not return within " + TIMEOUT_DURATION
								+ " seconds. This should not happen.");
					}

					// Third Write-Sync
					// Synchrnous Write
					data = new String(_zkBenchmark.getData() + i).getBytes();
					_client.setData().forPath(_path, data);
					_zkBenchmark.incrementFinished();

					// Synchronous Sync
					// 5 seconds for sync to return or we get false
					res = this.synchronousSync(Duration.ofSeconds(TIMEOUT_DURATION)); 
					if (!res) {
						System.out.println("Sync did not return within " + TIMEOUT_DURATION
								+ " seconds. This should not happen!!");
						LOG.error("Sync did not return within " + TIMEOUT_DURATION
								+ " seconds. This should not happen.");
					}

					// Synchronous Read
					_client.getData().forPath(_path);
					break;
				
				case AR:
				
					String mynode = "";
					String nodeName = "";
					try {
						mynode = _client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
								.forPath(_lockPath, new byte[0]);
						
						_zkBenchmark.incrementFinished();
						
						nodeName = mynode.substring(_lockRoot.length() + 1);
						LOG.info("Client #" + _id + " creates the node " + nodeName);
					} catch (Exception e) {
						LOG.error("Error: " + e);
					}

					while (true) {
						List<String> children = _client.getChildren().forPath(_lockRoot);

						_zkBenchmark.incrementFinished();

						Collections.sort(children);
						LOG.info("Client #" + _id + " views children: " + children);

						if (children.get(0).equals(nodeName)) {
							LOG.info("Client #" + _id + " acquires the lock " + nodeName);
							break;
						} else {
							int idx = children.indexOf(nodeName);
							String previousNode = _lockRoot + "/" + children.get(idx - 1);

							try {
								final CountDownLatch latch = new CountDownLatch(1);
								Stat stat = 
									_client.checkExists().usingWatcher(new CuratorWatcher() {
										@Override
										public void process(WatchedEvent event)
												throws Exception {
											// LOG.info("event: " + event);
											LOG.info("Counting down latch for: " + "Client #"
													+ _id);
											latch.countDown();
										}
									}).forPath(previousNode);

								_zkBenchmark.incrementFinished();

								if (stat != null) {
									LOG.info(
											"Client #" + _id + " watches the lock " + previousNode);
									latch.await();
								} else {
									LOG.info("stat is null for Client #" + _id + " checking " + previousNode
											+ ", immediately retry to acquire lock by calling getChildren" );
								}

							} catch (Exception e) {
								LOG.info("sync fails while client #" + _id + " tries to sync" + previousNode);
								// LOG.error("Error: " + e);
							}
						}
					}

					while (true) {
						try {
							_client.delete().forPath(mynode);

							_zkBenchmark.incrementFinished();

							LOG.info("Client #" + _id + " releases the lock " + nodeName);
						} catch (Exception e) {
							LOG.info("Client #" + _id + " fails to release the lock");
						}
						break;
					}
					break;

				case MUTEX:
				
					while (!_mutex.acquire((long) 1000, TimeUnit.MILLISECONDS)) {
						LOG.info("Client #" + _id + " fails to acquire the lock, retry");
					}
					LOG.info("Client #" + _id + " acquires the lock");
					_mutex.release();
					LOG.info("Client #" + _id + " releases the lock");
					break;

				case UNDEFINED:
					LOG.error("Test type was UNDEFINED. No tests executed");
					break;
				default:
					LOG.error("Unknown Test Type.");

			}

			recordElapsedInterval(new Double(submitTime));
			_count++; // increment _count to keep track of how many operations completed
			if (_type == TestType.AR) {
				_zkBenchmark.incrementLock();
			} else {
				_zkBenchmark.incrementFinished(); // increment the # of operations that have been completed
												// This is the aggregate global var across all threads
												// to keep track of the total # of operations ALL the threads
												// together have been able to complete

			}

			
			// _syncfin will be set to true when finish() is called.
			// This is called in BenchmarkClient under FinishTimer.
			// Finish Timer cancels the timer and then tells the sync client to stop issuing
			// requests by breaking out
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
	 * in fact, n here can be arbitrary number as synchronous operations can be stopped after
	 * finishing any operation.
	 */
	@Override
	protected void resubmit(int n) {
		_totalOps.getAndAdd(n);
	}

	/**
	 * Performs a blocking sync operation. Returns true if the sync completed normally, 
	 * false if it timed out or was interrupted.
	 */
	public boolean synchronousSync(Duration timeout) {
		CuratorFramework curator = _client;
		try {
			// Curator sync() is always a background operation. Use a latch to block until it
			// finishes.
			final CountDownLatch latch = new CountDownLatch(1);
			curator.sync().inBackground(new BackgroundCallback() {
				@Override
				public void processResult(CuratorFramework curator, CuratorEvent event)
						throws Exception {
					if (event.getType() == CuratorEventType.SYNC) {
						latch.countDown();
					}
				}
			}).forPath(_path);

			// Wait for sync to complete.
			return latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			System.out.println("Error: " + e);
			LOG.info("exception " + e);
			e.printStackTrace();
			return false;
		} catch (Exception e) {
			System.out.println("Error: " + e);
			LOG.info("exception " + e);
			e.printStackTrace();
			return false;
			// throw Throwables.propagate(e);
		}
	}
}
