package edu.brown.cs.zkbenchmark;

import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.LinkedList;
import java.util.Collections;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BrokenBarrierException;

import org.apache.zookeeper.data.Stat;
import org.apache.log4j.Logger;

import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncEventException;
import org.apache.curator.x.async.WatchMode;

import edu.brown.cs.zkbenchmark.ZooKeeperBenchmark.TestType;

public abstract class BenchmarkClient implements Runnable {
	protected Thread _thread;
	protected ZooKeeperBenchmark _zkBenchmark;
	protected String _host; 				// the host this client is connecting to
	protected CuratorFramework _client; 	// the actual client
	protected AsyncCuratorFramework _async; // the async wrapper
	protected TestType _type; 				// current test
	protected int _attempts;
	protected String _path;
	protected int _id;
	protected int _count;
	protected int _countTime;
	protected Timer _timer;
	protected List _lock;

	protected BufferedWriter _latenciesFile;

	private static final Logger LOG = Logger.getLogger(BenchmarkClient.class);

	public BenchmarkClient(ZooKeeperBenchmark zkBenchmark, String host, String namespace, int attempts, int id)
			throws IOException {
		_zkBenchmark = zkBenchmark; // The calling ZooKeeperBenchmark obj
		_host = host; 				// The specific client this server will connect to

		_client = CuratorFrameworkFactory.builder().connectString(_host).namespace(namespace)
				.retryPolicy(new RetryNTimes(Integer.MAX_VALUE, 1000)).connectionTimeoutMs(5000).build();
		_async = AsyncCuratorFramework.wrap(_client);
		_type = TestType.UNDEFINED;
		_attempts = attempts;
		_id = id;
		_path = "/client" + id;
		// _path = "/client-contention";
		_timer = new Timer();
		_lock = Collections.synchronizedList(new LinkedList());
	}

	@Override
	public void run() {
		if (!_client.isStarted())
			_client.start();

		if (_type == TestType.CLEANING) {
			doCleaning();
			return;
		}

		zkAdminCommand("srst");

		try {
			_zkBenchmark.getBarrier().await();
		} catch (InterruptedException e) {
			LOG.warn("Client #" + _id + " was interrupted while waiting on barrier", e);
		} catch (BrokenBarrierException e) {
			LOG.warn("Some other client was interrupted. Client #" + _id + " is out of sync", e);
		}

		_count = 0;
		_countTime = 0;

		try {
			Stat stat = _client.checkExists().forPath(_path);
			if (stat == null) {
				_client.create().forPath(_path, _zkBenchmark.getData().getBytes());
			}
		} catch (Exception e) {
			LOG.error("Error while creating working directory", e);
		}

		int interval = _zkBenchmark.getInterval();

		_timer.scheduleAtFixedRate(new FinishTimer(), interval, interval);

		try {
			if (_type == TestType.READ || _type == TestType.CREATE || _type == TestType.DELETE) {
				_latenciesFile = new BufferedWriter(new FileWriter(new File("results/last/" + _id + "-" + _type
																			+ "_timings.dat")));
			} else if (_type == TestType.MIXREADWRITE) {
				_latenciesFile = new BufferedWriter(new FileWriter(new File("results/last/" + _id + "-" + _type
																			+ "-" + this._zkBenchmark.getReadPercentage() + "_timings.dat")));
			}  else if (_type == TestType.AR) {
				_latenciesFile = new BufferedWriter(new FileWriter(new File("results/last/" + _id + "-" + _type
																			+ "_timings.dat")));
			} else {
				LOG.error("Unknown test type ");
			}
		} catch (IOException e) {
			LOG.error("Error while creating output file", e);
		}

		// Submit the requests! Blocking
		submit(_attempts, _type);

		zkAdminCommand("stat");

		try {
			if (_latenciesFile != null)
				_latenciesFile.close();
		} catch (IOException e) {
			LOG.warn("Error while closing output file:", e);
		}
		LOG.info("Client #" + _id + " -- Current test complete. " + "Completed " + _count + " operations.");

		_zkBenchmark.notifyFinished(_id);
	}

	class FinishTimer extends TimerTask {
		@Override
		public void run() {
			_countTime++;

			if (_countTime == _zkBenchmark.getDeadline()) {
				this.cancel(); 	// Cancel the current TimerTask and then call finish()
				finish(); 		// Abstract. Kill the sync or async client. Tell them to stop issuing requests
			}
		}
	}

	void doCleaning() {
		_zkBenchmark.notifyFinished(_id);
	}

	void recordEvent(CuratorEvent event) {
		ZooKeeperContext ctx = (ZooKeeperContext) event.getContext();
		// if (ctx.type != _type || ctx.ratio != _zkBenchmark.getReadPercentage()) {
		// 	System.out.println("** Error, context type is " + ctx.type + " while current type is " + _type);
		// 	System.out.println("** Error, context ratio is " + ctx.ratio + " while current ratio is " + _zkBenchmark.getReadPercentage());
		// }
		recordElapsedInterval(ctx.time);
	}

	void recordElapsedInterval(Double startTime) {
		double endtime = ((double) System.nanoTime() - _zkBenchmark.getStartTime()) / 1000000000.0;

		try {
			_latenciesFile.write(startTime.toString() + " " + Double.toString(endtime) + "\n");
		} catch (IOException e) {
			LOG.error("Exceptions while writing to file", e);
		}
	}

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
				LOG.info("Client #" + _id + " is sending " + cmd + " command:\n" + new String(b, 0, len));
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

	List getLock() {
		return _lock;
	}

	TestType getCurrentTest() {
		return _type;
	}

	void setThread(Thread thread) {
		_thread = thread;
	}

	abstract protected void submit(int n, TestType type);

	/**
	 * for synchronous requests, to submit more requests only needs to increase the
	 * total number of requests, here n can be an arbitrary number for asynchronous
	 * requests, to submit more requests means that the client will do both submit
	 * and wait
	 * 
	 * @param n
	 **/
	abstract protected void resubmit(int n);

	abstract protected void finish();
}
