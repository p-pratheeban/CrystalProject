package edu.mum.cs522.pratheeban.hybrid;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.mum.cs522.pratheeban.pair.Pair;

public class HybridReducer extends Reducer<Pair, IntWritable, Text, Text> {
	private int total = 0;
	private String currentTerm;
	private Map<String, Double> stripe = new HashMap<>();

	@Override
	protected void reduce(Pair key, Iterable<IntWritable> counts,
			Context context) throws IOException, InterruptedException {
		String first = key.getFirst().toString();
		String second = key.getSecond().toString();
		if (currentTerm == null) {
			currentTerm = first;
		} else if (!currentTerm.equals(first)) {//when new term w encountered
			Set<String> keys = stripe.keySet();
			for (String k : keys) {
				double relativeFrequency = stripe.get(k)/(double)total;
				stripe.put(k,relativeFrequency);			
			}

			emit(currentTerm, stripe, context);
			//reset for new term
			total = 0;
			currentTerm = first;
			stripe = new HashMap<String, Double>();
		}
		for (IntWritable count : counts) {
			Double oldValue = stripe.get(second);
			if (oldValue == null) {
				stripe.put(second, new Double(count.get()));
			} else {
				stripe.put(second, stripe.get(second) + count.get());
			}
			total += count.get();
		}	
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		//for the last term 
		Set<String> keys = stripe.keySet();
		for (String key : keys) {
			double relativeFrequency = stripe.get(key)/(double)total;
			stripe.put(key,relativeFrequency);			
		}
		emit(currentTerm, stripe, context);
	}
	
	private void emit(String term, Map<String, Double> stripe, Context context)
			throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		Set<String> keys = stripe.keySet();
		for (String key : keys) {
			sb.append("(");
			sb.append(key);
			sb.append(",");
			sb.append(stripe.get(key));
			sb.append(")");
		}
		sb.append("]");
		context.write(new Text(term), new Text(sb.toString()));
	}

}
