package edu.brown.cs.zkbenchmark;

import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;

// For config file parsing
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

// For config file getting
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

// For logging
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;

public class ZooKeeperBenchmark {
	private int _totalOps; 					// total operations requested by user
	private AtomicInteger _currentTotalOps; // possibly increased # of ops so test last for requested time
	private int _lowerbound;
	private BenchmarkClient[] _clients;
	private int _interval;
	private HashMap<Integer, Thread> _running;
	private AtomicInteger _finishedTotal;
	private int _lastfinished;
	private int _deadline; 					// in the unit of "_interval"
	private long _totalTimeSeconds;
	private long _lastCpuTime;
	private long _currentCpuTime;
	private long _startCpuTime;
	private TestType _currentTest;
	private String _data;
	private BufferedWriter _rateFile;
	private CyclicBarrier _barrier;
	private Boolean _finished;
	private double _readPercentage;
	private ArrayList<CuratorFramework> _curators;


	enum TestType {
		UNDEFINED, READ, WRITE, CREATE, DELETE, CLEANING, MIXREADWRITE, ACQUIRE, RELEASE, AR
	}

	private static final Logger LOG = Logger.getLogger(ZooKeeperBenchmark.class);

	public ZooKeeperBenchmark(Configuration conf) throws IOException {
		LinkedList<String> serverList = new LinkedList<String>();
		Iterator<String> serverNames = conf.getKeys("server");

		while (serverNames.hasNext()) {
			String serverName = serverNames.next();
			String address = conf.getString(serverName);
			serverList.add(address);
		}

		if (serverList.size() == 0) {
			throw new IllegalArgumentException("ZooKeeper server addresses required");
		}

		_interval = conf.getInt("interval");
		_totalOps = conf.getInt("totalOperations");
		_lowerbound = conf.getInt("lowerbound");
		int totaltime = conf.getInt("totalTime");
		_totalTimeSeconds = Math.round((double) totaltime / 1000.0);
		boolean sync = conf.getBoolean("sync");

		_running = new HashMap<Integer, Thread>();
		// ToDo, hardcode to 1 for debug
		// _clients = new BenchmarkClient[serverList.size()];
		_clients = new BenchmarkClient[1];
		_barrier = new CyclicBarrier(_clients.length + 1);
		_deadline = totaltime / _interval;

		LOG.info("benchmark set with: interval: " + _interval + " total number: " + _totalOps + " threshold: "
				+ _lowerbound + " time: " + totaltime + " sync: " + (sync ? "SYNC" : "ASYNC"));

		_data = "";
		for (int i = 0; i < 20; i++) {
			_data += "!!!!!";
		}

		int avgOps = _totalOps / serverList.size();

		// ToDo, hardcode to 1 for debug
		// for (int i = 0; i < serverList.size(); i++) {
		for (int i = 0; i < 1; i++) {
			if (sync) {
				// This is synchronous
				_clients[i] = new SyncBenchmarkClient(this, serverList.get(i), "zkTest", avgOps, i);
				// _curators.add(CuratorFrameworkFactory.builder().connectString(serverList.get(i)).namespace("/zkTest")
				// 			.retryPolicy(new RetryNTimes(Integer.MAX_VALUE, 1000)).connectionTimeoutMs(5000).build());
				// _clients[i].setCurator(_curators.get(i));
				// ListenerContainer<CuratorListener> tmp =  (ListenerContainer<CuratorListener>) _curators.get(i).getCuratorListenable();
				// tmp.addListener(new BenchmarkListener(_clients[i]));
			} else {
				// _clients[i] = new AsyncBenchmarkClient(this, serverList.get(i), "/zkTest", avgOps, i);
			}
		}

		_readPercentage = 0.0;
	}

	public void runBenchmark() {
		File directory = new File("./results/last/");
		if (!directory.exists()) {
			directory.mkdir();
		}

		System.out.println("-- " + _totalOps + " outstanding requests");

		/*
		 * Read requests are done by zookeeper extremely quickly compared with write
		 * requests. If the time interval and threshold are not chosen appropriately, it
		 * could happen that when the timer awakes, all requests have already been
		 * finished. In this case, the output of read test doesn't reflect the actual
		 * rate of read requests.
		 */
		// doTest(TestType.READ, "warm-up");

		// This loop increments i by 10% each time. i represents the read percentage. ie
		// the percentage of reads for this workload
		// for (int i = 0; i <= 100; i += 10) {
		// 	_readPercentage = i / 100.0;
		// 	doTest(TestType.MIXREADWRITE, "mixed read and write to znode");
		// }

		/*
		 * In the test, node creation and deletion tests are done by creating a lot of
		 * nodes at first and then deleting them. Since both of these two tests run for
		 * a certain time, there is no guarantee that which requests is more than the
		 * other. If there are more delete requests than create requests, the extra
		 * delete requests would end up not actually deleting anything. Though these
		 * requests are sent and processed by zookeeper server anyway, this could still
		 * be an issue.
		 */
		
		// doTest(TestType.DELETE, "znode delete");

		// ArrayList<ListenerContainer<CuratorListener>> _listeners = new ArrayList<ListenerContainer<CuratorListener>>(_clients.length);
		// for (int i=0; i < _clients.length; i++) {
		// 	ListenerContainer<CuratorListener> listeners = _clients[i].getListeners();
		// 	BenchmarkListener listener = new BenchmarkListener(_clients[i]);
		// 	listeners.addListener(listener);
		// 	// _listeners[i] = listeners;
		// }
		doTest(TestType.AR, "acquire-release");
		// for (int i=0; i< _clients.length; i++) {
		// 	// ToDo, clean
		// }


		LOG.info("Tests completed, now cleaning-up");

		// for (int i = 0; i < _clients.length; i++) {
		// 	_clients[i].setTest(TestType.CLEANING);
		// 	Thread tmp = new Thread(_clients[i]);
		// 	_running.put(new Integer(i), tmp);
		// 	tmp.start();
		// }

		while (!_finished) {
			synchronized (_running) {
				try {
					_running.wait();
				} catch (InterruptedException e) {
					LOG.warn("Benchmark main thread was interrupted while waiting", e);
				}
			}
		}

		LOG.info("All tests are completed");
	}

	/* This is where each individual test starts */
	public void doTest(TestType test, String description) {
		_currentTest = test;
		_finishedTotal = new AtomicInteger(0);
		_lastfinished = 0;
		_currentTotalOps = new AtomicInteger(_totalOps);
		_finished = false;

		System.out.println("-- running " + description + " benchmark for " + _totalTimeSeconds + " seconds... ");

		if (_currentTest == TestType.MIXREADWRITE) {
			System.out.print("and a read percentage of: " + _readPercentage * 100 + "% ");
		}

		try {
			if (_currentTest == TestType.READ || _currentTest == TestType.CREATE || _currentTest == TestType.DELETE) {
				_rateFile = new BufferedWriter(new FileWriter(new File("results/last/" + test + ".dat")));
			} else if (_currentTest == TestType.MIXREADWRITE) {
				_rateFile = new BufferedWriter(new FileWriter(new File("results/last/" + test + "-" + _readPercentage + ".dat")));
			} else if (_currentTest == TestType.AR) {
				_rateFile = new BufferedWriter(new FileWriter(new File("results/last/" + test + ".dat")));
			} else {
				LOG.error("Unknown test type");
			}

		} catch (IOException e) {
			LOG.error("Unable to create output file", e);
		}

		_startCpuTime = System.nanoTime();
		_lastCpuTime = _startCpuTime;
		
		for (int i = 0; i < _clients.length; i++) {
			_clients[i].setTest(test);
			Thread tmp = new Thread(_clients[i]);
			_running.put(new Integer(i), tmp);
			_clients[i].setThread(tmp);
			tmp.start();
		}

		try {
			_barrier.await();
		} catch (BrokenBarrierException e) {
			LOG.warn("Some other client was interrupted; Benchmark main thread is out of sync", e);
		} catch (InterruptedException e) {
			LOG.warn("Benchmark main thread was interrupted while waiting on barrier", e);
		}

		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new ResubmitTimer(), _interval, _interval);


		while (!_finished) {
				synchronized (_running) {
				try {
					_running.wait();
				} catch (InterruptedException e) {
					LOG.warn("Benchmark main thread was interrupted while waiting", e);
				}
			}
		}

		_currentTest = TestType.UNDEFINED;
		timer.cancel();

		try {
			if (_rateFile != null) {
				_rateFile.close();
			}
		} catch (IOException e) {
			LOG.warn("Error while closing output file", e);
		}

		double time = getTime();
		LOG.info(test + " finished, time elapsed (sec): " + time + " operations: " + _finishedTotal.get()
				+ " avg rate: " + _finishedTotal.get() / time);

		System.out.println("-- done");
	}

	/* return the max time consumed by each thread */
	double getTime() {
		double ret = 0;

		for (int i = 0; i < _clients.length; i++) {
			if (ret < _clients[i].getTimeCount())
				ret = _clients[i].getTimeCount();
		}

		return (ret * _interval) / 1000.0;
	}

	int getTotalOps() {
		/* return the total number of reqs done by all threads */
		int ret = 0;
		for (int i = 0; i < _clients.length; i++) {
			ret += _clients[i].getOpsCount();
		}
		return ret;
	}

	TestType getCurrentTest() {
		return _currentTest;
	}

	void incrementFinished() {
		_finishedTotal.incrementAndGet(); // There is no pure increment function. You must incrementAndGet
											// even if you don't intend on using the returned back value
	}

	CyclicBarrier getBarrier() {
		return _barrier;
	}

	String getData() {
		return _data;
	}

	int getDeadline() {
		return _deadline;
	}

	AtomicInteger getCurrentTotalOps() {
		return _currentTotalOps;
	}

	int getInterval() {
		return _interval;
	}

	double getReadPercentage() {
		return _readPercentage;
	}

	long getStartTime() {
		return _startCpuTime;
	}

	// Each thread will come back and tell _running to remove its id from the map
	void notifyFinished(int id) {
		synchronized (_running) {
			_running.remove(new Integer(id));
			// When _running size is 0, all thread have completed their requests
			// Set _finished to be true (used in the while loop) and notify _running (ie
			// wake it up)
			// and make it check that _finished is true
			if (_running.size() == 0) {
				_finished = true;
				_running.notify();
			}
		}
	}

	private static PropertiesConfiguration initConfiguration(String[] args) {
		OptionSet options = null;
		OptionParser parser = new OptionParser();
		PropertiesConfiguration conf = null;

		// Setup the option parser
		parser.accepts("help", "print this help statement");
		parser.accepts("conf", "configuration file (required)").withRequiredArg().ofType(String.class).required();
		parser.accepts("interval", "interval between rate measurements").withRequiredArg().ofType(Integer.class);
		parser.accepts("ops", "total number of operations").withRequiredArg().ofType(Integer.class);
		parser.accepts("lbound", "lowerbound for the number of operations").withRequiredArg().ofType(Integer.class);
		parser.accepts("time", "time tests will run for (milliseconds)").withRequiredArg().ofType(Integer.class);
		parser.accepts("sync", "sync or async test").withRequiredArg().ofType(Boolean.class);

		// Parse and gather the arguments
		try {
			options = parser.parse(args);
		} catch (OptionException e) {
			System.out.println("\nError parsing arguments: " + e.getMessage() + "\n");
			try {
				parser.printHelpOn(System.out);
			} catch (IOException e2) {
				LOG.error("Exception while printing help message", e2);
			}
			System.exit(-1);
		}

		Integer interval = (Integer) options.valueOf("interval");
		Integer totOps = (Integer) options.valueOf("ops");
		Integer lowerbound = (Integer) options.valueOf("lbound");
		Integer time = (Integer) options.valueOf("time");
		Boolean sync = (Boolean) options.valueOf("sync");

		// Load and parse the configuration file
		String configFile = (String) options.valueOf("conf");
		LOG.info("Loading benchmark from configuration file: " + configFile);

		try {
			conf = new PropertiesConfiguration(configFile);
		} catch (ConfigurationException e) {
			LOG.error("Failed to read configuration file: " + configFile, e);
			System.exit(-2);
		}

		// If there are options from command line, override the conf
		if (interval != null)
			conf.setProperty("interval", interval);
		if (totOps != null)
			conf.setProperty("totalOperations", totOps);
		if (lowerbound != null)
			conf.setProperty("lowerbound", lowerbound);
		if (time != null)
			conf.setProperty("totalTime", time);
		if (sync != null)
			conf.setProperty("sync", sync);

		return conf;
	}

	// Main where program execution starts
	public static void main(String[] args) {
		// Parse command line and configuration file
		PropertiesConfiguration conf = initConfiguration(args);

		// Helpful info for users of our default log4j configuration
		Appender a = Logger.getRootLogger().getAppender("file");
		if (a != null && a instanceof FileAppender) {
			FileAppender fa = (FileAppender) a;
			System.out.println("-- detailed logs going to: " + fa.getFile());
		}

		// Run the benchmark
		try {
			// Create zookeeper obj
			ZooKeeperBenchmark benchmark = new ZooKeeperBenchmark(conf);

			// Execute benchmark
			benchmark.runBenchmark();
		} catch (IOException e) {
			LOG.error("Failed to start ZooKeeper benchmark", e);
		}

		System.exit(0);
	}

	// TimerTask is a class that is used with Timer.
	// Allows for repeated execution by a Timer on a fixed interval
	class ResubmitTimer extends TimerTask {
		@Override
		public void run() {
			if (_currentTest == TestType.UNDEFINED) {
				return;
			}
			// _finished is an atomic integer. .get() of atomic integer gives back the value
			// int finished then represents the # of operations finished at the time
			// _finishedTotal.get() is called

			int finished = _finishedTotal.get();
			if (finished == 0) {
				return;
			}

			// Get current CPU time
			_currentCpuTime = System.nanoTime();

			// If output file exists
			if (_rateFile != null) {
				try {
					if (finished - _lastfinished > 0) {
						// Record the time elapsed and current rate
						String msg = ((double) (_currentCpuTime - _startCpuTime) / 1000000000.0) + " "
								+ ((double) (finished - _lastfinished)
										/ ((double) (_currentCpuTime - _lastCpuTime) / 1000000000.0));

						// Break it down:
						// This: ((double) (_currentCpuTime - _startCpuTime) / 1000000000.0)
						// represents the total time elapsed since _startCpuTime is called. 
						// So the total time (in seconds) since the barrier for thread

						// This: ((double) (finished - _lastfinished)
						// represents the # of requests finished since the last call to ResubmitTimer
						// So the total # of requests completed in this current time period

						// This: ((double) (_currentCpuTime - _lastCpuTime) / 1000000000.0))
						// represents the total time elapsed since the last call to ResubmitTimer (in seconds). 
						// So the total time elapsed for this current time period
						_rateFile.write(msg + "\n");
					}
				} catch (IOException e) {
					LOG.error("Error when writing to output file", e);
				}
			}

			_lastCpuTime = _currentCpuTime; // Set _lastCpuTime to current cpuTime
			_lastfinished = finished; 		// update _lastFinished to be the current # of finished requests

			// _currentTotalOps represents the total # of operations we want to submit by this point
			// Essentially it's the (# of intervals) * (avg operations/requests).
			// finished is the current total # of operations/requests that have been completed
			// numRemaining then means the number of requests BEHIND the ideal # of operations that should have been completed by now
			int numRemaining = _currentTotalOps.get() - finished;

			// If numRemaining requests is less than the lower bound, 
			// then we need to submit more requests.
			// _lowerbound is set in the config file
			if (numRemaining <= _lowerbound) {
				// Use some fomrula to compute what the new avg is and resubmit it to all the clients.
				// (essentially tell them to update the number of requests to send out at a time)
				int incr = _totalOps - numRemaining;

				_currentTotalOps.getAndAdd(incr);
				int avg = incr / _clients.length;

				for (int i = 0; i < _clients.length; i++) {
					_clients[i].resubmit(avg);	// Abstract. Essentially, add avg to the total # of operations/requests
												// client thread needs to complete
				}
			}
		}
	}
}
