//lab1 Areeg Mansour 20399122
package Pc;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinStockMapper extends Mapper<LongWritable, Text, Text,FloatWritable >{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FloatWritable>.Context context)
			throws IOException, InterruptedException {
		
		//Input
		//Key 			Value
		//LineNumber	complete line
		//1				How are you -- taken an example
		//LongWritable	Text (Hadoop Data type)
		try {
			String inputLine = value.toString();
			String[] miniStock = inputLine.split(",");
			
			//Array will be generated
		
			context.write(new Text(miniStock[0]), new FloatWritable (Float.parseFloat(miniStock[5])));
			
		}
		catch(NumberFormatException e){}
		
	
		//Output
		//Key 		Value
		//How		1 (every word with a value of 1)
		//Are		1
		//You		1
		//Text		IntWritable
	}
}
