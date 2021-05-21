package edu.brown.cs.zkbenchmark; // edu.ucsd.cse

import edu.brown.cs.zkbenchmark.ZooKeeperBenchmark.TestType;

public class ZooKeeperContext {
    public Double time;
    public TestType type;
    public Double ratio;
    public ZooKeeperContext(Double time, TestType type, Double ratio) {
        this.time = time;
        this.type = type;
        this.ratio = ratio;
    }
}
