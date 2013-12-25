package com.skunkworks;

import com.skunkworks.bolt.FileWriterBolt;
import com.skunkworks.spout.RandomLoglinesSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class SCTopology {

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("loglines", new RandomLoglinesSpout(),3);
		topologyBuilder.setBolt("fileWriter", new FileWriterBolt(), 3);
		
		Config conf = new Config();
	    conf.setDebug(true);
	    
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
