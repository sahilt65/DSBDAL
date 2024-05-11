import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Movies {
	// Output key of Mapper is moviewID -> Text
	// Ouput value of Mapper is userRating -> FloatWritable
	private static class MoviesMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException{
			if(values.toString().startsWith("userId")){
				// skip the line with column names
				return;
			}
			String[] lines = values.toString().split("\n");
			for(String line: lines){
				String[] data = line.split(",");
				Text movieId = new Text(data[1]);
				FloatWritable rating = new FloatWritable(Float.parseFloat(data[2]));
				context.write(movieId, rating);
			}
		}
	}
	
	private static class MoviesReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{
		private Text maxRated = new Text();
		private float maxRating = Float.MIN_VALUE;
		private TreeMap<Float, String>mp = new TreeMap<>();
		
		public void reduce(Text key, Iterable<FloatWritable> values, Context context){
			float sum =0;
			float ct = 0;
			for(FloatWritable ele: values){
				sum = sum + ele.get();
				ct++;
			}
			sum = (sum/ct);
			
			mp.put(sum, key.toString());
			
			if(sum > maxRating){
				maxRated.set(key.toString());
				maxRating = sum;
			}
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException{
			context.write(new Text("Movie with highest rating is: "), null);
			context.write(maxRated, new FloatWritable(maxRating));
			
			int ct = 0;
			
			// Printing the top 10 movies with highest rating
			context.write(new Text("\n\nTop 10 movies with highest rating: \n\n"), null);
			for(Map.Entry<Float, String> ele: mp.descendingMap().entrySet()){
				context.write(new Text(ele.getValue()), new FloatWritable(ele.getKey()));
				ct++;
				if(ct==10)break;
			}
		}
	}
	
	public static void main(String[] args)throws Exception{
		Configuration cnf = new Configuration();
		String[] files = new GenericOptionsParser(cnf, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		
		Job jb = new Job(cnf, "movies recommendation system");
		jb.setJarByClass(Movies.class);
		jb.setMapperClass(MoviesMapper.class);
		jb.setReducerClass(MoviesReducer.class);
		jb.setOutputKeyClass(Text.class);
		jb.setOutputValueClass(FloatWritable.class);
		
		FileInputFormat.addInputPath(jb, input);
		FileOutputFormat.setOutputPath(jb, output);
		
		System.exit(jb.waitForCompletion(true)? 0: 1);
	}

}
