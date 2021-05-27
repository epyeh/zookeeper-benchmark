package edu.brown.cs.zkbenchmark;

import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Random;
import java.util.List;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.apache.log4j.Logger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.x.async.WatchMode;

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
					String mynode = "";
					String nodeName = "";
					try {
						mynode = _client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(_lockPath, new byte[0]);
						nodeName = mynode.substring(1);
						LOG.info("Client #" + _id + " creates the node " + nodeName);
					} catch (Exception e) {
						LOG.error("Error: " + e);
					}
					
					while (true) {
						List<String> children = _client.getChildren().forPath("/");
						Collections.sort(children);
						LOG.info("Client #" + _id + " views children: " + children);
						
						if (children.get(0).equals(nodeName)) {
							LOG.info("Client #" + _id + " acquires the lock " + nodeName);
							break;
						} else {
							int idx = children.indexOf(nodeName);
							String previousNode = "/" + children.get(idx - 1);

							try {
								final CountDownLatch latch = new CountDownLatch(1);
								Stat stat = _client.checkExists().usingWatcher(new CuratorWatcher() {
									@Override
        							public void process(WatchedEvent event) throws Exception {
										LOG.info("event: " + event);
										// synchronized (_lock) {
										// LOG.info("Notifying: " + "Client #" + _id);
										// _lock.notify();
										// }
										LOG.info("Counting down latch for: " + "Client #" + _id);
										latch.countDown();
									}
								}).forPath(previousNode);

								if (stat != null) {
									LOG.info("Client #" + _id + " watches the lock " + previousNode);
									// synchronized (_lock) {
									// 	LOG.info("About to wait for: " + "Client #" + _id);
									// 	_lock.wait();
									// 	LOG.info("Past Wait for: " + "Client #" + _id);
									// }
									latch.await();
								} else {
									LOG.info("stat is null for: Client #" + _id + " immediately retry to acquire lock by calling getChildren");
								}

							} catch (Exception e) {
								LOG.error("Error: " + e);
							}
						}
					}

					while (true) {
						try {
							_client.delete().forPath(mynode);
							LOG.info("Client #" + _id + " releases the lock " + nodeName);
						} catch (Exception e) {
							LOG.info("Client #" + _id + " fails to release the lock");
						}
						break;
					}
					break;
					
				default:
					LOG.error("Unknown Test Type");

			}

			recordElapsedInterval(new Double(submitTime));
			_count++; 							
			_zkBenchmark.incrementFinished();

			if (_syncfin) {
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
