import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONObject;
//import org.apache.hadoop.io.Writable;

public class RedditAverage extends Configured implements Tool {

	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongPairWritable>{

		private LongPairWritable pair = new LongPairWritable();
		private Text word = new Text();
                //JSONObject record = new JSONObject();

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
		                JSONObject record = new JSONObject(value.toString());	        
				String s1 = (String)record.get("subreddit");  //Store the key
                                Integer i1 = (int) record.get("score");       //Store the value
                                pair.set(1,i1);                               //Initialize the first value in the pair
				word.set(s1);
                                context.write(word, pair);
			
		}
	}

	public static class IntSumReducer
	extends Reducer<Text,LongPairWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
			long sum1 = 0;
                        long sum2 = 0;
			for (LongPairWritable val : values) {
				sum1 += (long)val.get_0();      //Get the values from LongPairWritable and iterate them
                                sum2 += (long)val.get_1();
			}
                        double avg = 0;
                        avg=(double)sum2/(double)sum1;          //Calculate the average of values
                        result.set(avg);                        
			context.write(key, result);
		}
	}

      	public static class IntSumCombiner                      //The Combiner Optimization
	extends Reducer<Text,LongPairWritable, Text, LongPairWritable> {
		private LongPairWritable result = new LongPairWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
			long s1 = 0;
                        long s2 = 0;
			for (LongPairWritable val : values) {
				s1 += (long)val.get_0();
                                s2 += (long)val.get_1();
			}
                        LongPairWritable lpw = new LongPairWritable(s1,s2);
			context.write(key, lpw);
		}
	}        

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(RedditAverage.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumCombiner.class);
		job.setReducerClass(IntSumReducer.class);
                
                

                job.setMapOutputKeyClass(Text.class);                //Map output changes in the optimized code
                job.setMapOutputValueClass(LongPairWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}