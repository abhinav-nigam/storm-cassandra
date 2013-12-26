package com.skunkworks;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.bolt.CassandraCounterBatchingBolt;
import com.skunkworks.bolt.FileWriterBolt;
import com.skunkworks.spout.RandomLoglinesSpout;

public class SCTopology {

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		CassandraCounterBatchingBolt logPersistenceBolt = new CassandraCounterBatchingBolt("demo","CassandraLocal","users", "user", "increment" );
		
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("loglines", new RandomLoglinesSpout(),3);
		topologyBuilder.setBolt("fileWriter", new FileWriterBolt(), 3).shuffleGrouping("loglines");
		topologyBuilder.setBolt("cassandraWriter", logPersistenceBolt, 3).shuffleGrouping("fileWriter");
		
		Map<String, Object> cassandraConfig = new HashMap<String, Object>();
	    cassandraConfig.put(StormCassandraConstants.CASSANDRA_HOST, "localhost:9160");
	    cassandraConfig.put(StormCassandraConstants.CASSANDRA_KEYSPACE, "demo");
		Config conf = new Config();
		conf.put("CassandraLocal", cassandraConfig);
	    
	    if (args != null && args.length > 0) {
	        conf.setNumWorkers(3);

	        StormSubmitter.submitTopology(args[0], conf, topologyBuilder.createTopology());
	      }
	      else {

	        LocalCluster cluster = new LocalCluster();
	        cluster.submitTopology("test", conf, topologyBuilder.createTopology());
	        Utils.sleep(10000);
	        cluster.killTopology("test");
	        cluster.shutdown();
	      }
	}
}
