//lab1 Areeg Mansour 20399122
package Pc;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MinStockReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{

	@Override
	protected void reduce(Text key, Iterable<FloatWritable> values,
			Reducer<Text, FloatWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
		//input 
		//Key,		List of Values
		//How		{1,1,1}
		
		float miniNumber = Float.MAX_VALUE;
		for (FloatWritable value : values) {
			miniNumber = Math.min(miniNumber, value.get());
		}

		context.write(key, new FloatWritable(miniNumber));
		}
		
		
	
}
