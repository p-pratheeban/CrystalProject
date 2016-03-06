package edu.mum.cs522.pratheeban.pair;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PairPartitioner extends Partitioner<Pair, IntWritable> {

	@Override
	public int getPartition(Pair key, IntWritable value, int numReduceTasks) {
		return key.getFirst().hashCode() % numReduceTasks;
	}

}
