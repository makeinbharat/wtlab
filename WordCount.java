import java.io.*;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount extends Configured implements Tool{
	
	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new WordCount(), args);
		System.exit(res);
	}
	
	public int run(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
		Job job = Job.getInstance(getConf(), "wordcount");
		job.setJarByClass(this.getClass());
		
		FileInputFormat.addInputPath(job, new Path("input.txt"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		return job.waitForCompletion(true)?0:1;
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		Pattern WORD_B = Pattern.compile("\\s*\\b\\s*");
		
		public void map(LongWritable offset, Text lines, Context context) throws IOException, InterruptedException {
			String line = lines.toString();
			Text cw = new Text();
			
			for(String w : WORD_B.split(line)){
				if(w.isEmpty())
					continue;
				cw = new Text(w);
				context.write(cw, new IntWritable(1));
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable c : counts){
				sum+=c.get();
			}
			context.write(word, new IntWritable(sum));
		}
	}	
}