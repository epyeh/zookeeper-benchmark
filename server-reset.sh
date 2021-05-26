cd ~
rm -r apache-zookeeper-3.7.0-bin
tar -xf apache-zookeeper-3.7.0-bin.tar.gz
cd apache-zookeeper-3.7.0-bin/
mkdir .zk
touch ./.zk/myid
echo 2 > ./.zk/myid

rm ./conf/zoo_sample.cfg
touch ./conf/zoo.cfg

cfg="tickTime=2000
initLimit=10
syncLimit=5
dataDir=/home/ubuntu/apache-zookeeper-3.7.0-bin/.zk
# clientPort=2181

4lw.commands.whitelist=stat, srst

server.1=172.31.29.168:2888:3888;2181
server.2=172.31.16.243:2888:3888;2181
server.3=172.31.31.160:2888:3888;2181"

echo "$cfg" > ./conf/zoo.cfg
cd ~