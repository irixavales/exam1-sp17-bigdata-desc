package edu.uprm.cse.bigdata.p1exam1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KeywordToTweetsMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		// keywords to be counted
		String[] keywords = {"flu", "zika", "diarrhea", "ebola", "swamp", "change"};
		// split the comma separated input value by attributes
		String[] cols = value.toString().split(",", 2);
		// quick fix to avoid dirty data. i will come back to this later
		if(cols.length!=2)
			return;
		// get id of tweet
		String id = cols[0];		
		// get text of tweet
		String text = cols[1];
		String lowercase_text = text.toLowerCase();
		// loop through keywords to see if they appear in the text
		for (String word: keywords) {
			if (lowercase_text.contains(word)) {
				context.write(new Text(word), new Text(id));
			}
		}		
	}
}
