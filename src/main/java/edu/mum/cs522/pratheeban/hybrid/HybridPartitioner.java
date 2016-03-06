package edu.mum.cs522.pratheeban.hybrid;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import edu.mum.cs522.pratheeban.pair.Pair;

public class HybridPartitioner extends Partitioner<Pair, IntWritable> {

	@Override
	public int getPartition(Pair key, IntWritable value, int numReduceTasks) {
		return key.getFirst().hashCode() % numReduceTasks;
	}

}
