package WordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		//Input
		//Key 			Value
		//LineNumber	complete line
		//1				How are you -- taken an example
		//LongWritable	Text (Hadoop Data type)
		String inputLine = value.toString();
		String[] wordArray = inputLine.split(" ");
		//Array will be generated
		//[0] -->How
		//[1] -->are
		//[2] -->you
		for(String word : wordArray)
		{
			//generate Mapper output using context
			//for each word, we have occurance 1 in the array index, so hardcoding the value 1
			context.write(new Text(word), new IntWritable (1));
		}
		//Output
		//Key 		Value
		//How		1 (every word with a value of 1)
		//Are		1
		//You		1
		//Text		IntWritable
	}
}
