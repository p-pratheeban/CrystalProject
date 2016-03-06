package edu.mum.cs522.pratheeban.stripe;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class StripePartitioner extends Partitioner<Text, MapWritable> {

	@Override
	public int getPartition(Text key, MapWritable value, int numReduceTasks) {
		return key.hashCode() % numReduceTasks;
	}

}
