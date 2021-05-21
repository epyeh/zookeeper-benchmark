package edu.brown.cs.zkbenchmark; // edu.ucsd.cse

import edu.brown.cs.zkbenchmark.ZooKeeperBenchmark.TestType;

public class ZooKeeperContext {
    public Double time;
    public TestType type;
    public ZooKeeperContext(Double time, TestType type) {
        this.time = time;
        this.type = type;
    }
}
