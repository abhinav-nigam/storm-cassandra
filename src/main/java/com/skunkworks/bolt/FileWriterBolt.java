package com.skunkworks.bolt;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class FileWriterBolt extends BaseRichBolt{
    OutputCollector _collector;
    PrintWriter pw;
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		try {
			pw = new PrintWriter("/tmp/storm-cass.txt", "UTF-8");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void execute(Tuple input) {
		pw.println(input.getString(0));
		_collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
	
	@Override
	public void cleanup(){
		pw.flush();
		pw.close();
		super.cleanup();
	}
	

}
