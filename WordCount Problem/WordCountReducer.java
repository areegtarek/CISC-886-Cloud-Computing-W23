package WordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		//input 
		//Key,		List of Values
		//How		{1,1,1}
		
		Integer wordCount = 0;
		for(IntWritable count : values)
		{
			wordCount += count.get();
		}
		//wordCount : 3
		
		context.write(key, new IntWritable(wordCount));
	}	
}
