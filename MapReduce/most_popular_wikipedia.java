import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikipediaPopular extends Configured implements Tool {
	
	public static class WikiPopMapper
    extends Mapper<LongWritable, Text, Text, LongWritable>{
 
        @Override
        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
            String fileTitle = ((FileSplit) context.getInputSplit()).getPath().getName(); //Get the file name (from inside pagecounts-*)
            String dateTime = fileTitle.substring(11, 22); //Get the date and time which is imbibed in the file name between position 11 and 22
            
            String entry = value.toString();  //Remove pages not in English
            if (entry.startsWith("en")) {            
            	String[] container = entry.split(" +");
            	String title = container[1];        //Ignore main_page and special pages
            	if (!title.equals("Main_Page") && !title.startsWith("Special:")) {
            		LongWritable viewCount = new LongWritable(Long.parseLong(container[2]));  //This method obtains any 'longs' in the string            		
            		context.write(new Text(dateTime), viewCount);
            	}
            }
        }
    }
	
	public static class WikiPopReducer
	extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();
		
		@Override
        public void reduce(Text key, Iterable<LongWritable> values,
                Context context
                ) throws IOException, InterruptedException {
            long maxView = 1;      //Get the value which is viewed maximum
            for (LongWritable val : values) {
                if (maxView < val.get()) maxView = val.get();
         
         }
            result.set(maxView);
            context.write(key, result);
        }
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
        System.exit(res);
	}
	
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "wikipedia popular");
        job.setJarByClass(WikipediaPopular.class);
 
        job.setInputFormatClass(TextInputFormat.class);
 
        job.setMapperClass(WikiPopMapper.class);
        job.setCombinerClass(WikiPopReducer.class);
        job.setReducerClass(WikiPopReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
