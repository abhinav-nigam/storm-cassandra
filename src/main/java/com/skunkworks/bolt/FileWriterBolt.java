package com.skunkworks.bolt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class FileWriterBolt extends BaseRichBolt{
    OutputCollector _collector;
    BufferedWriter bw;
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		try {
			bw = new BufferedWriter(new FileWriter(new File("/tmp/storm-cass.txt")));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void execute(Tuple input) {
		try {
			bw.write(input.getString(0));
			_collector.emit(input, new Values(input));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));		
	}
	
	@Override
	public void cleanup(){
		try {
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

}
