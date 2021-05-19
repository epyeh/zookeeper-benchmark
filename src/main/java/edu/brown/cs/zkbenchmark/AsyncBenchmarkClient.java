package edu.brown.cs.zkbenchmark;

import java.io.File;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Random;
import java.lang.Math;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.framework.listen.ListenerContainer;

import edu.brown.cs.zkbenchmark.ZooKeeperBenchmark.TestType;

public class AsyncBenchmarkClient extends BenchmarkClient {

	private class Monitor {
	}

	TestType _currentType = TestType.UNDEFINED;
	private Monitor _monitor = new Monitor();
	private boolean _asyncRunning;

	private static final Logger LOG = Logger.getLogger(AsyncBenchmarkClient.class);

	public AsyncBenchmarkClient(ZooKeeperBenchmark zkBenchmark, String host, String namespace, int attempts, int id)
			throws IOException {
		super(zkBenchmark, host, namespace, attempts, id);
	}

	@Override
	protected void submit(int n, TestType type) {
		ListenerContainer<CuratorListener> listeners = (ListenerContainer<CuratorListener>) _client
				.getCuratorListenable();
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

		// BufferedWriter readWriteDecisionFile = null;
		// if (type == TestType.MIXREADWRITE) {
		// readWriteDecisionFile = new BufferedWriter(new FileWriter(new File(
		// "results/" + _id + "-" + _type + this._zkBenchmark.getReadPercentage() +
		// "-read-write.dat")));
		// }

		// int numToRead = this._zkBenchmark.getNumToRead();
		// int numToWrite = 20 - numToRead;

		// int currRead = numToRead;
		// int currWrite = numToWrite;

		// Random rand = new Random();
		// int randInt = rand.nextInt(100);

		for (int i = 0; i < n; i++) {
			double time = ((double) System.nanoTime() - _zkBenchmark.getStartTime()) / 1000000000.0;

			switch (type) {
				case READ:
					_client.getData().inBackground(new Double(time)).forPath(_path);
					break;

				case SETSINGLE:
					data = new String(_zkBenchmark.getData() + i).getBytes();
					_client.setData().inBackground(new Double(time)).forPath(_path, data);
					break;

				case SETMULTI:
					data = new String(_zkBenchmark.getData() + i).getBytes();
					_client.setData().inBackground(new Double(time)).forPath(_path + "/" + (_count % _highestN), data);
					break;

				case CREATE:
					data = new String(_zkBenchmark.getData() + i).getBytes();
					_client.create().inBackground(new Double(time)).forPath(_path + "/" + _count, data);
					_highestN++;
					break;

				case DELETE:
					_client.delete().inBackground(new Double(time)).forPath(_path + "/" + _count);
					_highestDeleted++;

					if (_highestDeleted >= _highestN) {
						zkAdminCommand("stat");
						_zkBenchmark.notifyFinished(_id);
						_timer.cancel();
						_count++;
						return;
					}
					break;

				// Case for trying to do mixed reads and writes to nodes
				case MIXREADWRITE:
					// System.out.println("Made it");
					// _client.getData().inBackground(new Double(time)).forPath(_path);

					double randDouble = Math.random();
					double readThreshold = this._zkBenchmark.getReadPercentage();
					// if (Double.compare(readThreshold, 0.0) == 0) {
					// data = new String(_zkBenchmark.getData() + i).getBytes();
					// _client.setData().inBackground(new Double(time)).forPath(_path, data);
					// } else if (Double.compare(readThreshold, 1.0) == 0) {
					// _client.getData().inBackground(new Double(time)).forPath(_path);
					// } else if (randDouble < readThreshold) {
					// // _readWriteDecisionFile.write("read\n");
					// // _client.getData().inBackground(new Double(time)).forPath(_path);
					// // _client.getData().inBackground(new Double(time)).forPath(_path + "/read");
					// _client.getData().inBackground(new Double(time)).forPath(_path);
					// } else {
					// // _readWriteDecisionFile.write("write\n");
					// data = new String(_zkBenchmark.getData() + i).getBytes();
					// // _client.setData().inBackground(new Double(time)).forPath(_path, data);
					// // _client.setData().inBackground(new Double(time)).forPath(_path + "/write",
					// // data);
					// _client.setData().inBackground(new Double(time)).forPath(_path, data);
					// }

					// _client.getData().inBackground(new Double(time)).forPath(_path);

					if (randDouble < readThreshold) {
						// _readWriteDecisionFile.write("read\n");
						// _client.getData().inBackground(new Double(time)).forPath(_path);
						// _client.getData().inBackground(new Double(time)).forPath(_path);
						_client.getData().inBackground(new Double(time)).forPath(_path);

					} else {
						// _readWriteDecisionFile.write("write\n");
						data = new String(_zkBenchmark.getData() + i).getBytes();
						// _client.setData().inBackground(new Double(time)).forPath(_path, data);
						// _client.setData().inBackground(new Double(time)).forPath(_path, data);
						_client.setData().inBackground(new Double(time)).forPath(_path, data);
					}

					// if (currRead != 0) {
					// _readWriteDecisionFile.write("read\n");
					// _client.getData().inBackground(new Double(time)).forPath(_path);
					// currRead--;
					// } else {
					// _readWriteDecisionFile.write("write\n");
					// data = new String(_zkBenchmark.getData() + i).getBytes();
					// _client.setData().inBackground(new Double(time)).forPath(_path, data);
					// // data = new String(_zkBenchmark.getData() + i).getBytes();
					// // _client.setData().forPath(_path + "/write", data);
					// currWrite--;
					// }

					// if (currRead == 0 && currWrite == 0) {
					// currRead = numToRead;
					// currWrite = numToWrite;
					// }

					break;

				// case UNDEFINED:
				// LOG.error("Test type was UNDEFINED. No tests executed");
				// break;
				// default:
				// LOG.error("Unknown Test Type.");
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
