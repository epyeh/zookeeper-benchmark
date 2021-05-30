package edu.brown.cs.zkbenchmark;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BrokenBarrierException;

import org.apache.zookeeper.data.Stat;
import org.apache.log4j.Logger;

import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import edu.brown.cs.zkbenchmark.ZooKeeperBenchmark.TestType;

public abstract class BenchmarkClient implements Runnable {
	protected ZooKeeperBenchmark _zkBenchmark;
	protected String _host; // the host this client is connecting to
	protected CuratorFramework _client; // the actual client
	protected TestType _type; // current test
	protected int _attempts;
	protected String _path;
	protected int _id;
	protected int _count;
	protected int _countTime;
	protected Timer _timer;
	protected String _lockRoot = "/lock-root";
	protected String _lockPath = _lockRoot + "/lock-";
	protected BufferedWriter _latenciesFile;
	protected InterProcessMutex _mutex;

	private static final Logger LOG = Logger.getLogger(BenchmarkClient.class);

	// Simple constructor
	public BenchmarkClient(ZooKeeperBenchmark zkBenchmark, String host, String namespace,
			int attempts, int id) throws IOException {
		_zkBenchmark = zkBenchmark; // The calling ZooKeeperBenchmark obj
		_host = host;
		_client = CuratorFrameworkFactory.builder().connectString(_host).namespace(namespace)
				.retryPolicy(new RetryNTimes(Integer.MAX_VALUE, 1000)).connectionTimeoutMs(5000)
				.build();

		_type = TestType.UNDEFINED;
		_attempts = attempts; // This is avgOps. The average # of operations to send to the server
		_id = id; // This clients index. Essentially, what server does this client connect to and
					// handle?
		// _path = "/client" + id;
		_path = "/contention";
		_timer = new Timer();
		_mutex = new InterProcessMutex(_client, _lockRoot);
	}

	// Where multithread execution begins
	@Override
	public void run() {
		// If client not started, start it
		if (!_client.isStarted())
			_client.start();

		// If cleaning, then execute doCleaning (not sure what cleaning means)
		if (_type == TestType.CLEANING) {
			doCleaning();
			return;
		}

		if (_path.equals("/contention")) {
			if (_id == 0) {
				try {
					Stat stat = _client.checkExists().forPath(_path);
					if (stat == null) {
						_client.create().forPath(_path, _zkBenchmark.getData().getBytes());
					}
				} catch (Exception e) {
					LOG.error("Cannot create working directory '/contention'");
				}
			}
		}

		zkAdminCommand("srst"); // Reset ZK server's statistics

		// Wait for all client threads to be ready. Block at barrier
		try {
			_zkBenchmark.getBarrier().await();
		} catch (InterruptedException e) {
			LOG.warn("Client #" + _id + " was interrupted while waiting on barrier", e);
		} catch (BrokenBarrierException e) {
			LOG.warn("Some other client was interrupted. Client #" + _id + " is out of sync", e);
		}

		_count = 0;
		_countTime = 0;

		// Create a directory to work in
		try {
			if (_path.equals("/contention")) {
				// do nothing
			} else {
				Stat stat = _client.checkExists().forPath(_path);
				if (stat == null) {
					_client.create().forPath(_path, _zkBenchmark.getData().getBytes());
				}
				if (_id == 0) {
					stat = _client.checkExists().forPath(_lockRoot);
					if (stat == null) {
						_client.create().forPath(_lockRoot, new byte[0]);
					}
				}
			}
		} catch (Exception e) {
			LOG.error("Error while creating working directory", e);
		}

		// Create a timer to check when we're finished. Schedule it to run
		// periodically in case we want to record periodic statistics
		int interval = _zkBenchmark.getInterval();

		// After _interval milliseconds have passed, execute the function
		// FinishTimer()
		_timer.scheduleAtFixedRate(new FinishTimer(), interval, interval);

		// Create a new output file for this particular client
		try {
			if (_type == TestType.READ || _type == TestType.WRITESYNCREAD || _type == TestType.AR || _type == TestType.MUTEX) {
				_latenciesFile = new BufferedWriter(new FileWriter(
						new File("results/last/" + _id + "-" + _type + "_timings.dat")));
			} else if (_type == TestType.MIXREADWRITE) {
				_latenciesFile = new BufferedWriter(
						new FileWriter(new File("results/last/" + _id + "-" + _type + "-"
								+ this._zkBenchmark.getReadPercentage() + "_timings.dat")));
			} else {
				LOG.error("Unknown test type");
			}
		} catch (IOException e) {
			LOG.error("Error while creating output file", e);
		}

		// Submit the requests!
		// Blocking!
		submit(_attempts, _type); // Abstract


		// Test is complete. Print some stats and go home.
		// Zookeeper server keeps track of some stats. This zkAdminCommand
		// Tells zookeeper to execute the "stat" command and give the stats
		// back to this running thread
		zkAdminCommand("stat");

		// Clean up by closing files and logging completion time
		try {
			if (_latenciesFile != null) {
				_latenciesFile.close();
			}
		} catch (IOException e) {
			LOG.warn("Error while closing output file:", e);
		}

		LOG.info("Client #" + _id + " -- Current test complete. " + "Completed " + _count
				+ " operations.");

		// Notify ZooKeeperBenchmark object that this thread is done computing
		_zkBenchmark.notifyFinished(_id);
	}

	// TimerTask is a class that is used with Timer. Allows for repeated
	// execution by a Timer on a fixed interval
	// Just increment _countTime every _interval milliseconds
	// Recall, _deadline is computed as: totaltime / _interval
	// So if totaltime = 30000 and _interval = 200
	// Then 30000/200 = 150.
	// 150 represents the total number of times the clock needs to tick (with an
	// interval of _interval)
	// So each time _interval milliseconds pass, we tick countTime by 1.
	//
	// Note, they could just have easily kept a timer where it keeps track of the
	// aggregate milliseconds that passed
	// ie time+=_interval. When time == totalTime -> exit out
	class FinishTimer extends TimerTask {
		@Override
		public void run() {
			// this can be used to measure rate of each thread
			// at this moment, it is not necessary
			_countTime++;

			if (_countTime == _zkBenchmark.getDeadline()) {
				this.cancel(); // Cancel the current TimerTask and then call finish()
				finish(); // Abstract. Kill the sync or async client. ie tell them to stop issuing
							// requests
			}
		}
	}

	void doCleaning() {
		try {
			deleteChildren();
		} catch (Exception e) {
			LOG.error("Exception while deleting old znodes", e);
		}

		_zkBenchmark.notifyFinished(_id);
	}

	/* Delete all the child znodes created by this client */
	void deleteChildren() throws Exception {
		List<String> children;
		
		// do {
		// 	children = _client.getChildren().forPath(_path);
		// 	for (String child : children) {
		// 		_client.delete().inBackground().forPath(_path + "/" + child);
		// 	}
		// 	Thread.sleep(2000);
		// } while (children.size() != 0);

		if (_id == 0) {
			do {
				children = _client.getChildren().forPath("/");
				System.out.println("To Clean: " + children);
				for (String child : children) {
					try {
						_client.delete().forPath("/" + child);
					} catch (Exception e) {
						LOG.info(child + " has been deleted");
					}
				}
			} while (children.size() != 0);
		}
	}

	void recordEvent(CuratorEvent event) {
		Double submitTime = (Double) event.getContext();
		recordElapsedInterval(submitTime);
	}

	// startTime here represents the time when ONE request was sent
	void recordElapsedInterval(Double startTime) {
		// compute endtime which is when ONE request finishes.
		double endtime = ((double) System.nanoTime() - _zkBenchmark.getStartTime()) / 1000000000.0;

		// Write start and end latencies to output file
		try {
			_latenciesFile.write(startTime.toString() + " " + Double.toString(endtime) + "\n");
		} catch (IOException e) {
			LOG.error("Exceptions while writing to file", e);
		}
	}

	/* Send a command directly to the ZooKeeper server */
	void zkAdminCommand(String cmd) {
		String host = _host.split(":")[0];
		int port = Integer.parseInt(_host.split(":")[1]);
		Socket socket = null;
		OutputStream os = null;
		InputStream is = null;
		byte[] b = new byte[1000];

		try {
			socket = new Socket(host, port);
			os = socket.getOutputStream();
			is = socket.getInputStream();

			os.write(cmd.getBytes());
			os.flush();

			int len = is.read(b);
			while (len >= 0) {
				LOG.info("Client #" + _id + " is sending " + cmd + " command:\n"
						+ new String(b, 0, len));
				len = is.read(b);
			}

			is.close();
			os.close();
			socket.close();
		} catch (UnknownHostException e) {
			LOG.error("Unknown ZooKeeper server: " + _host, e);
		} catch (IOException e) {
			LOG.error("IOException while contacting ZooKeeper server: " + _host, e);
		}
	}

	int getTimeCount() {
		return _countTime;
	}

	int getOpsCount() {
		return _count;
	}

	ZooKeeperBenchmark getBenchmark() {
		return _zkBenchmark;
	}

	void setTest(TestType type) {
		_type = type;
	}

	abstract protected void submit(int n, TestType type);

	/**
	 * for synchronous requests, to submit more requests only needs to increase the total number of
	 * requests, here n can be an arbitrary number for asynchronous requests, to submit more
	 * requests means that the client will do both submit and wait
	 * 
	 * @param n
	 */
	abstract protected void resubmit(int n);

	abstract protected void finish();
}
