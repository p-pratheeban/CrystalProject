package edu.mum.cs522.pratheeban.pair;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PairMapper extends Mapper<LongWritable, Text, Pair, IntWritable> {
	
	private Map<Pair, Integer> mapOutput;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		mapOutput = new HashMap<>();
	}

	@Override
	protected void map(LongWritable docid, Text doc, Context context)
			throws IOException, InterruptedException {
		String records = doc.toString();
		String[] inputSplits = records.split("//\n");
		for (int k = 0; k < inputSplits.length; k++) {
			String[] inputSplit = inputSplits[k].split(" ");
			for (int i = 0; i < inputSplit.length - 1; i++) {
				Pair starPair = new Pair(inputSplit[i], "*");
				for (int j = i + 1; j < inputSplit.length; j++) {
					if (inputSplit[i].equals(inputSplit[j])) {
						break;
					}
					// Add a star pair to the mapOutput
					if (mapOutput.get(starPair) == null) {
						mapOutput.put(starPair, 1);
					} else {
						mapOutput.put(starPair, mapOutput.get(starPair) + 1);
					}
					// Add a pair to the mapOutout
					Pair pair = new Pair(inputSplit[i], inputSplit[j]);
					if (mapOutput.get(pair) == null) {
						mapOutput.put(pair, 1);
					} else {
						mapOutput.put(pair, mapOutput.get(pair) + 1);
					}
				}
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		Set<Pair> keys = mapOutput.keySet();
		for (Pair p : keys) {
			context.write(p, new IntWritable(mapOutput.get(p)));
		}
	}

}
