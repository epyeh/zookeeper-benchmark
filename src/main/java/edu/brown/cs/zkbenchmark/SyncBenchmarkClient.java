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

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.apache.log4j.Logger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.BackgroundCallback;

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
					String mynode = _client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(_lockPath, new byte[0]);
					String nodeName = mynode.substring(1);
					LOG.info("Client #" + _id + " creates a node: " + nodeName);
					while (true) {
						_client.sync().forPath("/");
						List<String> children = _client.getChildren().forPath("/");
						Collections.sort(children);
						LOG.info("Client #" + _id + " views children: " + children);
						if (children.get(0).equals(nodeName)) {
							break;
						} else {
							int idx = children.indexOf(nodeName);
							LOG.info("Client #" + _id + " watches " + children.get(idx-1));
							Stat stat = _client.checkExists().watched().inBackground(new BackgroundCallback() {
								@Override
								public void processResult(CuratorFramework curator, CuratorEvent event) throws Exception {
									synchronized (_lock) {
										_lock.notify();
									}
								}
							}).forPath("/"+children.get(idx-1));

							// _async.watched().checkExists().forPath("/"+children.get(idx-1)).event().thenAccept(event -> {
							// 	LOG.info("Client #" + _id + " wakes up by " + event.getType() + " of " + children.get(idx-1));
							// 	synchronized (_lock) {
							// 		_lock.notify();
							// 	}
							// 	((CompletionStage<WatchedEvent>) event).exceptionally(exception -> {
							// 		LOG.warn("Client #" + _id + " should not happen");
							// 		return null;
							// 	});

							// });
							if (stat != null) {
								synchronized (_lock) {
									_lock.wait();
								}
							}
						}
					}

					while (true) {
						try {
							_client.delete().forPath(mynode);
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
