package edu.mum.cs522.pratheeban.pair;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PairReducer extends
		Reducer<Pair, IntWritable, Pair, DoubleWritable> {
	int total;

	@Override
	protected void reduce(Pair key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		if (key.getSecond().toString().equals("*")) {
			total = 0;
			for (IntWritable count : values) {
				total += count.get();
			}
		} else {
			int sum = 0;
			for (IntWritable count : values) {
				sum += count.get();
			}
			double relativeFrequency = sum / (double) total;
			context.write(key, new DoubleWritable(relativeFrequency));
		}

	}
}
