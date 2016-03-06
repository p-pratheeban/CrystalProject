package edu.mum.cs522.pratheeban.stripe;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class StripeReducer extends Reducer<Text, MapWritable, Text, Text> {
	
	@Override
	protected void reduce(Text key, Iterable<MapWritable> stripes,
			Context context) throws IOException, InterruptedException {
		MapWritable mapWritable = new MapWritable();
		int total = 0;
		for (MapWritable stripe : stripes) {
			Set<Writable> keys = stripe.keySet();
			for (Writable k : keys) {
				Text text = (Text) k;
				IntWritable count = (IntWritable) stripe.get(text);
				if (mapWritable.get(text) == null) {
					mapWritable.put(text, count);
				} else {
					IntWritable count1 = (IntWritable) mapWritable.get(text);
					mapWritable.put(text,
							new IntWritable(count.get() + count1.get()));
				}
				total += count.get();
			}
		}
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		Set<Writable> keys = mapWritable.keySet();
		for (Writable k : keys) {
			Text text = (Text) k;
			IntWritable count = (IntWritable) mapWritable.get(text);
			sb.append("(");
			sb.append(text.toString());
			sb.append(",");
			double relativeFrequency = count.get() / (double) total;
			sb.append(relativeFrequency);
			sb.append(")");
		}
		sb.append("]");
		context.write(key, new Text(sb.toString()));
	
	}
	
	
}
