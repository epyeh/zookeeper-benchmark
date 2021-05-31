package edu.brown.cs.zkbenchmark;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.BrokenBarrierException;

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
	private int _totalOps; // total operations requested by user
	private AtomicInteger _currentTotalOps;
	private int _lowerbound;
	private BenchmarkClient[] _clients;
	private int _interval;
	private HashMap<Integer, Thread> _running;
	private AtomicInteger _finishedTotal;
	private AtomicInteger _lockTotal;
	private int _lastfinished;
	private int _deadline; // in units of "_interval"
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
	private static int cToS; // represents the number of clients to servers. 2 means 2 clients for 1
								// server

	enum TestType {
		READ, CLEANING, UNDEFINED, MIXREADWRITE, WRITESYNCREAD, AR, MUTEX
	}

	private static final Logger LOG = Logger.getLogger(ZooKeeperBenchmark.class);

	public ZooKeeperBenchmark(Configuration conf) throws IOException {
		LinkedList<String> serverList = new LinkedList<String>();

		// Config file is structured: key=value
		// totalTime=30000
		// conf.get(totalTime) would give back 30000
		// This command gets all the keys that have server in their name
		Iterator<String> serverNames = conf.getKeys("server");

		// Add server addresses to list
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
		cToS = conf.getInt("clientsToServer");

		_running = new HashMap<Integer, Thread>(); // Hashmap of currently running threads
		// _clients = new BenchmarkClient[serverList.size()]; // Array of clients that connect to
		// zookeeper servers. 1
		// client per server
		// ERIC: Why do they only have 1 client per server?
		// Does this make sense?
		_clients = new BenchmarkClient[cToS * serverList.size()];
		_barrier = new CyclicBarrier(_clients.length + 1); // THIS IS NECESSARY TO SYNCHRONIZE ALL
															// CLIENTS and the
															// 'main' doTest function in this class.
															// Only once _clients.length + 1 calls
															// to _barrier.await()
															// have executed, are ALL the threads
															// released to begin
															// making zookeeper requests
															// it's _clients.length + 1 because
															// doTest will call it also
		_deadline = totaltime / _interval; // interval is the time between request rate measurements
											// totaltime/ _interval gives the # of measurments
											// we want to take

		LOG.info("benchmark set with: interval: " + _interval + " total number: " + _totalOps
				+ " threshold: " + _lowerbound + " time: " + totaltime + " sync: "
				+ (sync ? "SYNC" : "ASYNC"));

		// Create the data to be stored in the znodes
		// ERIC: They don't vary data size. Interesting.
		_data = "";

		for (int i = 0; i < 20; i++) { // 100 bytes of important data
			_data += "!!!!!";
		}

		// Calculate the average # of operations that need to be sent to each zookeeper server
		int avgOps = _totalOps / (cToS * serverList.size());

		// Instantiate 1 client for each server
		for (int i = 0; i < cToS * serverList.size(); i++) {
			int idx = i % serverList.size();
			if (sync) {
				// This is synchronous
				_clients[i] =
						new SyncBenchmarkClient(this, serverList.get(idx), "zkTest", avgOps, i);
			} else {
				// _clients[i] =
				// new AsyncBenchmarkClient(this, serverList.get(idx), "zkTest", avgOps, i);
			}
		}

	}


	// Executes the benchmark
	public void runBenchmark() {

		// Create results directory if it doesn't exist
		File directory = new File("./results/last");
		if (!directory.exists()) {
			directory.mkdir();
		}

		/*
		 * Read requests are done by zookeeper extremely quickly compared with write requests. If
		 * the time interval and threshold are not chosen appropriately, it could happen that when
		 * the timer awakes, all requests have already been finished. In this case, the output of
		 * read test doesn't reflect the actual rate of read requests.
		 */

		doTest(TestType.READ, "warm-up");

		// doTest(TestType.READ, "znode read"); // Do twice to allow for warm-up

		// This loop increments i by 10% each time. i represents the read percentage. ie
		// the percentage of reads for this workload
		// for (int i = 0; i <= 100; i += 10) {
		// 	_readPercentage = i / 100.0;
		// 	doTest(TestType.MIXREADWRITE, "mixed read and write to znode");
		// }

		// doTest(TestType.MUTEX, "mutex");

		doTest(TestType.WRITESYNCREAD, "repeated write sync read");

		// doTest(TestType.AR, "acquire-release");

		LOG.info("Tests completed, now cleaning-up");

		_finished = false;
		for (int i = 0; i < _clients.length; i++) {
			_clients[i].setTest(TestType.CLEANING);
			Thread tmp = new Thread(_clients[i]);
			_running.put(new Integer(i), tmp);
			tmp.start();
		}

		while (!_finished) {
			synchronized (_running) {
				try {
					_running.wait();
				} catch (InterruptedException e) {
					LOG.warn("Benchmark main thread was interrupted while waiting", e);
				}
			}
		}

		LOG.info("Cleaning-up completed");
	}


	/* This is where each individual test starts */
	public void doTest(TestType test, String description) {

		_currentTest = test;
		_finishedTotal = new AtomicInteger(0);
		_lastfinished = 0;
		_currentTotalOps = new AtomicInteger(_totalOps);
		_finished = false;
		if (test == TestType.AR) {
			_lockTotal = new AtomicInteger(0);
		}

		System.out.print(
				"Running " + description + " benchmark for " + _totalTimeSeconds + " seconds... ");

		if (_currentTest == TestType.MIXREADWRITE) {
			System.out.print(" and a readPercentage of:" + _readPercentage + "% ");
		}

		// Instantiate rate output file. This will be where the results go
		try {
			if (_currentTest == TestType.READ || _currentTest == TestType.WRITESYNCREAD
					|| _currentTest == TestType.AR || _currentTest == TestType.MUTEX) {
				_rateFile = new BufferedWriter(
						new FileWriter(new File("results/last/" + test + ".dat")));
			} else if (_currentTest == TestType.MIXREADWRITE) {
				_rateFile = new BufferedWriter(new FileWriter(
						new File("results/last/" + test + "-" + _readPercentage + ".dat")));
			} else {
				LOG.error("Unknown test type");
			}
		} catch (IOException e) {
			LOG.error("Unable to create output file", e);
		}

		// These are just timestamps. This is not the actual timer
		_startCpuTime = System.nanoTime();
		_lastCpuTime = _startCpuTime;

		// Start the testing clients!
		for (int i = 0; i < _clients.length; i++) {
			_clients[i].setTest(test); // Set the test type
			Thread tmp = new Thread(_clients[i]); // Create a new thread object
			_running.put(new Integer(i), tmp); // Store the index and the thread object into the map
			tmp.start(); // Start the thread
		}

		// Wait for clients to connect to their assigned server, and
		// start timer which ensures we have outstanding requests.
		try {
			_barrier.await();
		} catch (BrokenBarrierException e) {
			LOG.warn("Some other client was interrupted; Benchmark main thread is out of sync", e);
		} catch (InterruptedException e) {
			LOG.warn("Benchmark main thread was interrupted while waiting on barrier", e);
		}

		// Start the timer
		Timer timer = new Timer();

		// Yaosen, No Need to Resubmit, Just Sample
		timer.scheduleAtFixedRate(new SampleTimer(), _interval, _interval);

		// Wait for the test to finish
		while (!_finished) {
			// must be synchronized because multiple threads may try to delete
			// their id from the map at the same time so we want to avoid concurrent write problem
			synchronized (_running) {
				try {
					_running.wait();
				} catch (InterruptedException e) {
					LOG.warn("Benchmark main thread was interrupted while waiting", e);
				}
			}
		}

		// Test is finished
		// Clean up test
		_currentTest = TestType.UNDEFINED;
		timer.cancel();

		// Close files
		try {
			if (_rateFile != null) {
				if (test == TestType.AR) {
					_rateFile.write("\n" + "#total-acquire-release: " + _lockTotal.get());
				}
				_rateFile.close();
			}
		} catch (IOException e) {
			LOG.warn("Error while closing output file", e);
		}

		// Print out total time taken for test
		double time = getTime();
		LOG.info(test + " finished, time elapsed (sec): " + time + " operations: "
				+ _finishedTotal.get() + " avg rate: " + _finishedTotal.get() / time);

		System.out.println("-- done");
	}

	TestType getCurrentTest() {
		return _currentTest;
	}

	void incrementFinished() {
		_finishedTotal.incrementAndGet(); // There is no pure increment function. 
											// You must incrementAndGet
											// even if you don't intend on using the returned back value
	}

	void incrementLock() {
		_lockTotal.incrementAndGet();
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
		return this._readPercentage;
	}

	long getStartTime() {
		return _startCpuTime;
	}

	// Each thread will come back and tell _running to remove its id from the map
	void notifyFinished(int id) {
		synchronized (_running) {
			_running.remove(new Integer(id));
			// When _running size is 0, all thread have completed their requests
			// Set _finished to be true (used in the while loop) and notify _running
			// (ie wake it up)
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
		parser.accepts("conf", "configuration file (required)").withRequiredArg()
				.ofType(String.class).required();
		parser.accepts("interval", "interval between rate measurements").withRequiredArg()
				.ofType(Integer.class);
		parser.accepts("ops", "total number of operations").withRequiredArg().ofType(Integer.class);
		parser.accepts("lbound", "lowerbound for the number of operations").withRequiredArg()
				.ofType(Integer.class);
		parser.accepts("time", "time tests will run for (milliseconds)").withRequiredArg()
				.ofType(Integer.class);
		parser.accepts("sync", "sync or async test").withRequiredArg().ofType(Boolean.class);
		parser.accepts("clientsToServer", "Number of clients to server").withRequiredArg()
				.ofType(Integer.class);

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
			System.out.println("Detailed logs going to: " + fa.getFile());
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

	// TimerTask is a class that is used with Timer. Allows for repeated
	// execution by a Timer on a fixed interval
	class SampleTimer extends TimerTask {
		@Override
		public void run() {
			if (_currentTest == TestType.UNDEFINED) {
				return;
			}
			// _finished is an atomic integer. .get() of atomic integer gives back the value
			// finished then represents the # of operations finished at the time
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
						String msg =
								((double) (_currentCpuTime - _startCpuTime) / 1000000000.0) + " "
										+ ((double) (finished - _lastfinished) / ((double) (_currentCpuTime - _lastCpuTime) / 1000000000.0));

						// Break it down:
						// This: ((double) (_currentCpuTime - _startCpuTime) / 1000000000.0)
						// represents the total time elapsed since _startCpuTime is called.
						// So the total time (in seconds) since the barrier for thread

						// This: ((double) (finished - _lastfinished)
						// represents the # of requests finished since the last call to
						// ResubmitTimer
						// So the total # of requests completed in this current time period

						// This: ((double) (_currentCpuTime - _lastCpuTime) / 1000000000.0))
						// represents the total time elapsed since the last call to ResubmitTimer
						// (in seconds).
						// So the total time elapsed for this current time period
						_rateFile.write(msg + "\n");
					}
				} catch (IOException e) {
					LOG.error("Error when writing to output file", e);
				}
			}

			_lastCpuTime = _currentCpuTime; // Set _lastCpuTime to current cpuTime
			_lastfinished = finished; // update _lastFinished to be the current # of finished
										// requests
		}
	}
}
