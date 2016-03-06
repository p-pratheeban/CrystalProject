package edu.mum.cs522.pratheeban.stripe;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class StripeMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
	private Map<Text, MapWritable> mapperOutput;

	@Override
	public void setup(Context context) {
		mapperOutput = new HashMap<Text, MapWritable>();
	}

	@Override
	protected void map(LongWritable docid, Text doc, Context context)
			throws IOException, InterruptedException {
		String records = doc.toString();
		String[] inputSplits = records.split("//\n");
		for (int k = 0; k < inputSplits.length; k++) {
			String[] inputSplit = inputSplits[k].split(" ");
			for (int i = 0; i < inputSplit.length - 1; i++) {
				Text key = new Text(inputSplit[i]);
				MapWritable stripe = new MapWritable();
				for (int j = i + 1; j < inputSplit.length; j++) {
					if (inputSplit[i].equals(inputSplit[j])) {
						break;
					}
					Text value = new Text(inputSplit[j]);
					if (stripe.get(value) == null) {
						stripe.put(value, new IntWritable(1));
					} else {
						IntWritable count = (IntWritable) stripe.get(value);
						stripe.put(value, new IntWritable(count.get() + 1));
					}

				}
				if (mapperOutput.containsKey(key)) {
					MapWritable tempStripe = combine(
							mapperOutput.get(new Text(key)), stripe);
					mapperOutput.put(new Text(key), tempStripe);
				} else {
					mapperOutput.put(new Text(key), stripe);
				}

			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {

		Set<Text> keys = mapperOutput.keySet();
		for (Text key : keys) {
			MapWritable stripe = (MapWritable) mapperOutput.get(key);
			context.write(key, stripe);
		}

	}

	public MapWritable combine(MapWritable m1, MapWritable m2) {
		Set<Writable> keys = m2.keySet();
		for (Writable k : keys) {
			Text text = (Text) k;
			IntWritable count2 = (IntWritable) m2.get(text);
			if (m1.containsKey(text)) {
				IntWritable count1 = (IntWritable) m1.get(text);
				m1.put(text, new IntWritable(count1.get() + count2.get()));
			} else {
				m1.put(text, count2);
			}
		}
		return m1;
	}

}
