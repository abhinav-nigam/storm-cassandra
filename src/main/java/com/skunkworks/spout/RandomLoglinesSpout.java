package com.skunkworks.spout;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class RandomLoglinesSpout extends BaseRichSpout{
	SpoutOutputCollector _collector;
	Random _rand;
	
	public void open(Map conf, TopologyContext context,
		SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
	}

	public void nextTuple() {
		Utils.sleep(10);
		String[] videoTitles = new String[]{"Gangnam Style", "Harlem Shake",
				"What the fox says"};
		String[] users = new String[]{"Tom", "Dick", "Harry"};
		StringBuffer line = new StringBuffer("" + System.currentTimeMillis());
		line.append(",").append(users[_rand.nextInt(users.length)]).append(",")
						.append(videoTitles[_rand.nextInt(videoTitles.length)]);
		_collector.emit(new Values(line.toString()));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
		
	}
	
}
