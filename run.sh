mvn clean install
mvn package
cat /dev/null > zk-benchmark.log
java -cp target/lib/*:target/* edu.brown.cs.zkbenchmark.ZooKeeperBenchmark --conf benchmark.conf
