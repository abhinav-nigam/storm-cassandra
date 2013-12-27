package com.skunkworks.bolt;

import static backtype.storm.utils.Utils.tuple;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
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
		String line = input.getString(0);
		String[] tokens = line.split(",");
		pw.println(input.getString(0));
		_collector.emit(tuple(tokens[0] + "," + tokens[1], tokens[2], tokens[3], tokens[4], tokens[5], tokens[6]));
		_collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key", "views", "likes", "dislikes", "comments", "favourites"));
	}
	
	@Override
	public void cleanup(){
		pw.flush();
		pw.close();
		super.cleanup();
	}
	

}
