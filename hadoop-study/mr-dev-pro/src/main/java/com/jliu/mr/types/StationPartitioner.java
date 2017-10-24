package com.jliu.mr.types;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import com.jliu.common.NcdcRecordParser;

public class StationPartitioner extends Partitioner<LongWritable, Text> {
	private NcdcRecordParser parser = new NcdcRecordParser();

	@Override
	public int getPartition(LongWritable key, Text value, int numPartitions) {
		parser.parse(value.toString());

		return getPartition(parser.getStationId());
	}

	private int getPartition(String stationId) {
		return 0;
	}
}
