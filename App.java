package com.test.begin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Test Map -Reduce
 *
 */
public class App{
	
	public static class TestMap extends Mapper<Object, Text, Text, IntWritable>{
		//The output value of mapper is kept 1 of type IntWritable, as no specific Object value of use
		public final static IntWritable occurence = new IntWritable(1);
		
		public Text word = new Text();

		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.map(key, value, context);
			// Under this function you can read each seperate line as VALUE, and dig out the information you ned to pass as key to reducer
			// if you know delimeter, you can easily extract tokens by value.toString().split("ddelimeter")
			//or you can use StringTokenizer to iterate through the tokens
			// you can select any number of key,value pairs to output
			
			// Assuming that I use AllstarFull.csv where records are seperated by ,
			
			String[] words = value.toString().split(",");
			word.set(words[0]);
			context.write(word, occurence);
			
		}
		
		// SO, TestMap should now return key value pairs after reading the AllstarFull.csv like(playerId, 1)
		// and its gonna read each line and output one key,value pair per line
		
	}
	
	public static class TestReduce extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text arg0, Iterable<IntWritable> arg1,
				Reducer<Text, IntWritable, Text, IntWritable>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.reduce(arg0, arg1, arg2);
			
			int sum = 0;
			for(IntWritable value : arg1){
				
				sum += value.get();
				
			}
			
			arg2.write(arg0, new IntWritable(sum));
		}
		
		
	}
	
    public static void main( String[] args ) throws Exception
    {
       
    	
    	Configuration conf = new Configuration();
    	
    	Job job = Job.getInstance(conf, "player count");
    	
    	job.setJarByClass(App.class);
    	
    	job.setMapperClass(TestMap.class);
    	
    	job.setCombinerClass(TestReduce.class);
    	
    	job.setReducerClass(TestReduce.class);
    	
    	job.setOutputKeyClass(Text.class);
    	
    	job.setOutputValueClass(IntWritable.class);
    	
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	
    	System.exit(job.waitForCompletion(true)?0:1);
    }
}
