package edu.mum.cs522.pratheeban.pair;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PairDriver extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}

		Job job = Job.getInstance(getConf());
		job.setJobName("Pairs approach to compute relative frequencies");
		job.setJarByClass(PairDriver.class);
		
		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Pair.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(PairMapper.class);
		job.setPartitionerClass(PairPartitioner.class);
		job.setReducerClass(PairReducer.class);

		
		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);
		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);

		/*
		 * Delete output filepath if already exists
		 */
		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		PairDriver pairDriver = new PairDriver();
		int res = ToolRunner.run(pairDriver, args);
		System.exit(res);
	}
}