package com.skunkworks;

import java.util.ArrayList;
import java.util.Collection;
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
import com.hmsonline.storm.cassandra.bolt.AckStrategy;
import com.hmsonline.storm.cassandra.bolt.CassandraCounterBatchingBolt;
import com.hmsonline.storm.cassandra.bolt.TransactionalCassandraBatchBolt;
import com.hmsonline.storm.cassandra.bolt.mapper.DefaultTupleMapper;
import com.skunkworks.bolt.FileWriterBolt;
import com.skunkworks.spout.YoutubeSpout;

public class SCTopology {

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        Collection<String> keyspaces = new ArrayList<String>();
        keyspaces.add("demo");
		Map<String, Object> cassandraConfig = new HashMap<String, Object>();
	    cassandraConfig.put(StormCassandraConstants.CASSANDRA_HOST, "localhost:9160");
	    cassandraConfig.put(StormCassandraConstants.CASSANDRA_KEYSPACE, keyspaces);
		Config conf = new Config();
		conf.setDebug(true);
		conf.put("CassandraLocal", cassandraConfig);
		
		CassandraCounterBatchingBolt userBolt = new CassandraCounterBatchingBolt("demo","CassandraLocal","users", "user", "increment" );
		CassandraCounterBatchingBolt songBolt = new CassandraCounterBatchingBolt("demo","CassandraLocal","songs", "song", "increment" );
		TransactionalCassandraBatchBolt videoBolt = new TransactionalCassandraBatchBolt("CassandraLocal", new DefaultTupleMapper("demo", "videos", "key"));
		videoBolt.setAckStrategy(AckStrategy.ACK_ON_WRITE);
		
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		//topologyBuilder.setSpout("loglines", new RandomLoglinesSpout(),3);
		topologyBuilder.setSpout("loglines", new YoutubeSpout(),1);
		//topologyBuilder.setBolt("fileWriter", new FileWriterBolt(), 1).shuffleGrouping("loglines");
		topologyBuilder.setBolt("cassandraWriter", videoBolt, 1).shuffleGrouping("loglines");
		//topologyBuilder.setBolt("cassandraUserWriter", userBolt, 3).shuffleGrouping("fileWriter");
		//topologyBuilder.setBolt("cassandraSongWriter", songBolt, 3).shuffleGrouping("fileWriter");
	    
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
