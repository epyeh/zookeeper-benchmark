package edu.brown.cs.zkbenchmark;

import java.util.Random;
import java.lang.Math;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.framework.listen.ListenerContainer;

import edu.brown.cs.zkbenchmark.ZooKeeperBenchmark.TestType;

public class AsyncBenchmarkClient extends BenchmarkClient {

	private class Monitor {}

	private TestType _currentType = TestType.UNDEFINED;
	private Monitor _monitor = new Monitor();
	private boolean _asyncRunning;

	private static final Logger LOG = Logger.getLogger(AsyncBenchmarkClient.class);

	public AsyncBenchmarkClient(ZooKeeperBenchmark zkBenchmark, String host, String namespace, int attempts, int id) throws IOException {
		super(zkBenchmark, host, namespace, attempts, id);
	}

	@Override
	protected void submit(int n, TestType type) {
		ListenerContainer<CuratorListener> listeners = (ListenerContainer<CuratorListener>) _client.getCuratorListenable();
		BenchmarkListener listener = new BenchmarkListener(this);
		listeners.addListener(listener);
		_currentType = type;
		_asyncRunning = true;

		submitRequests(n, type);

		synchronized (_monitor) {
			while (_asyncRunning) {
				try {
					_monitor.wait();
				} catch (InterruptedException e) {
					LOG.warn("AsyncClient #" + _id + " was interrupted", e);
				}
			}
		}

		listeners.removeListener(listener);
	}

	private void submitRequests(int n, TestType type) {
		try {
			submitRequestsWrapped(n, type);
		} catch (Exception e) {
			// What can you do? for some reason
			// com.netflix.curator.framework.api.Pathable.forPath() throws Exception

			// just log the error, not sure how to handle this exception correctly
			LOG.error("Exception while submitting requests", e);
		}
	}

	private void submitRequestsWrapped(int n, TestType type) throws Exception {
		byte data[];

		for (int i = 0; i < n; i++) {
			double time = ((double) System.nanoTime() - _zkBenchmark.getStartTime()) / 1000000000.0;

			switch (type) {
				case READ:
					_client.getData().inBackground(new ZooKeeperContext(time, TestType.READ, 0.0)).forPath(_path);
					break;

				// Case for trying to do mixed reads and writes to nodes
				case MIXREADWRITE:
					double randDouble = Math.random();
					double readThreshold = this._zkBenchmark.getReadPercentage();

					if (randDouble < readThreshold) {
						_client.getData().inBackground(new ZooKeeperContext(time, TestType.MIXREADWRITE, readThreshold)).forPath(_path);
					} else {
						data = new String(_zkBenchmark.getData() + i).getBytes();
						_client.setData().inBackground(new ZooKeeperContext(time, TestType.MIXREADWRITE, readThreshold)).forPath(_path, data);
					}
					break;

				case UNDEFINED:
					LOG.error("Test type was UNDEFINED. No tests executed");
					break;
				default:
					LOG.error("Unknown Test Type");
			}
			_count++;
		}

	}

	@Override
	protected void finish() {
		synchronized (_monitor) {
			_asyncRunning = false;
			_monitor.notify();
		}
	}

	@Override
	protected void resubmit(int n) {
		submitRequests(n, _currentType);
	}
}
