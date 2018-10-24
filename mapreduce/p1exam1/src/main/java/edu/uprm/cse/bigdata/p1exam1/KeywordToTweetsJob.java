package edu.uprm.cse.bigdata.p1exam1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KeywordToTweetsJob {

	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
			System.err.println("Usage: KeywordToTweets <input path> <output path>");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf);

		job.setJobName("Keyword To Tweets");
		job.setJarByClass(KeywordToTweetsJob.class);
		job.setMapperClass(KeywordToTweetsMapper.class);
		job.setReducerClass(KeywordToTweetsReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		Path outputPath = new Path(args[1]);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		outputPath.getFileSystem(conf).delete(outputPath, true);


		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
