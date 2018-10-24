package edu.uprm.cse.bigdata.p1exam1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KeywordToTweetsReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		// key: keyword
		// values: id's of tweets in which keyword appears
		
		// String of id's separated by comma
		String id_list = values.iterator().next().toString();
		// iterate over list of id's, to append them
		while (values.iterator().hasNext()) {
			id_list += ", ";
			id_list += values.iterator().next().toString();
		}
		
		context.write(key, new Text(id_list));
	}

}
