package WordCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
	
public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	Configuration conf = new Configuration();
	//Define MapReduce job
	Job job = new Job(conf);
	//start from this class 
	job.setJarByClass(WordCountDriver.class);
	//Set Mapper and Reduce classes
	job.setMapperClass(WordCountMapper.class);
	//set the number of reducers 
	job.setNumReduceTasks(1);
	job.setReducerClass(WordCountReducer.class);
	
	//Set output types
	job.setOutputKeyClass(Text.class); //word//Text
	job.setOutputValueClass(IntWritable.class); //3//IntWritable
	
	//Set input and output locations
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	//Delete Output Directory if exists 
	FileSystem fs = FileSystem.get(conf);
	fs.delete(new Path(args[1]));
	//Submit job
	job.waitForCompletion(true);
	
}
}