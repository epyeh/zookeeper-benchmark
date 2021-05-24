package edu.brown.cs.zkbenchmark;

// import com.netflix.curator.framework.CuratorFramework;
// import com.netflix.curator.framework.api.CuratorEvent;
// import com.netflix.curator.framework.api.CuratorEventType;
// import com.netflix.curator.framework.api.CuratorListener;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;

import edu.brown.cs.zkbenchmark.ZooKeeperBenchmark.TestType;

class BenchmarkListener implements CuratorListener {
	private BenchmarkClient _client; // client listener listens for

	BenchmarkListener(BenchmarkClient client) {
		_client = client;
	}

	@Override
	public void eventReceived(CuratorFramework client, CuratorEvent event) {
		CuratorEventType type = event.getType();
		// Ensure that the event is reply to current test
		if ((type == CuratorEventType.GET_DATA && _client.getBenchmark().getCurrentTest() == TestType.READ)
				|| (type == CuratorEventType.SET_DATA && _client.getBenchmark().getCurrentTest() == TestType.SETMULTI)
				|| (type == CuratorEventType.SET_DATA && _client.getBenchmark().getCurrentTest() == TestType.SETSINGLE)
				|| (type == CuratorEventType.DELETE && _client.getBenchmark().getCurrentTest() == TestType.DELETE)
				|| (type == CuratorEventType.CREATE && _client.getBenchmark().getCurrentTest() == TestType.CREATE)
				|| (type == CuratorEventType.GET_DATA
						&& _client.getBenchmark().getCurrentTest() == TestType.MIXREADWRITE)
				|| (type == CuratorEventType.SET_DATA
						&& _client.getBenchmark().getCurrentTest() == TestType.MIXREADWRITE)
				|| (type == CuratorEventType.GET_DATA
						&& _client.getBenchmark().getCurrentTest() == TestType.WRITESYNCREAD)) {
			_client.getBenchmark().incrementFinished();
			_client.recordEvent(event);
			_client.resubmit(1);
		}

		// It must be like this for WRITESYNCREAD because we don't want the write or sync call to generate
		// another WRITESYNCREAD request. This would lead to an infinite number of requests
		// because each request will make 3 more leading to out of memory error
		// Instead, ONLY the last call (read) can resubmit a new request. This is why this if block is here
		// For more info on CuratorListener: https://curator.apache.org/apidocs/org/apache/curator/framework/api/CuratorListener.html
		// For more info on CuratorEventType: https://curator.apache.org/apidocs/org/apache/curator/framework/api/CuratorEventType.html
       if ((type == CuratorEventType.SET_DATA
						&& _client.getBenchmark().getCurrentTest() == TestType.WRITESYNCREAD)
			|| (type == CuratorEventType.SYNC
						&& _client.getBenchmark().getCurrentTest() == TestType.WRITESYNCREAD)){
			_client.getBenchmark().incrementFinished();
			_client.recordEvent(event);
		}
	}
}
