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


public class MusicSkipped {
	public static class MusicSkippedMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private static final IntWritable RADIO = new IntWritable(1);
		private static final IntWritable SKIP = new IntWritable(2);
		
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException{
			String[] lines = values.toString().split("\n");
			for(String line: lines){
				String[] data = line.split(",");
				Text trackId = new Text(data[1]);
				int skipped = Integer.parseInt(data[3]);
				int radio = Integer.parseInt(data[4]);
				if(skipped == 1)
					context.write(trackId, SKIP);
				if(radio == 1)
					context.write(trackId, RADIO);
			}
		}
	}
	
	public static class MusicSkippedReducer extends Reducer<Text, IntWritable, Text, Text>{
		private final static Text res = new Text();
		public void reduce(Text trackid, Iterable<IntWritable> args, Context context) throws IOException, InterruptedException{
			int radioct=0, skippedct=0;
			for(IntWritable val : args){
				int ct = val.get();
				if(ct==1)
					radioct++;
				if(ct==2)
					skippedct++;					
			}
			String output = "Listened on Radio: " + radioct + ", Skipped: " + skippedct;
			res.set(output);
			context.write(trackid, res);
		}
	}
	
	public static void main(String[] args)throws Exception{
		Configuration c = new Configuration();
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		
		Job j = new Job(c, "MusicSkipped");
		j.setJarByClass(MusicSkipped.class);
		j.setMapperClass(MusicSkippedMapper.class);
		j.setReducerClass(MusicSkippedReducer.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		
		System.exit(j.waitForCompletion(true)? 0: 1);
	}
}
