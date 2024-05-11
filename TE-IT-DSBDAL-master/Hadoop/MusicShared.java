import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


	
public class MusicShared {
	public static class MusicMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException{
			String[] filef = values.toString().split("\n");
			for(String line: filef){
				String[] data = line.split(",");
				Text trackid = new Text(data[1]);
				IntWritable shared = new IntWritable(Integer.parseInt(data[2]));
				context.write(trackid, shared);
			}
		}
	}
	public static class MusicReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable val: values){
				sum += val.get();
			}
			con.write(word, new IntWritable(sum));
		}		
	}
	public static void main(String[] args) throws Exception{
		Configuration c = new Configuration();
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		
		Job j = new Job(c, "MusicShared");
		j.setJarByClass(MusicShared.class);
		j.setMapperClass(MusicMapper.class);
		j.setReducerClass(MusicReducer.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		
		System.exit(j.waitForCompletion(true) ? 0: 1);
		
	}
}
