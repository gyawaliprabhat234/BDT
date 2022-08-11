package first.lab3.priya.a;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TemperatureAverage extends Configured implements Tool{
	
	public static class TemperatureAverageMapper extends 
	Mapper<LongWritable, Text, Text, DoubleWritable>{
		private Text date = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			date.set(value.toString().substring(15, 19));
			Double temperatureDouble = Double.parseDouble(value.toString().substring(87, 92))/10.0;
			DoubleWritable temperature = new DoubleWritable(temperatureDouble);
			context.write(date, temperature);
			
		}
		
	}
	
	public static class TemperatureAverageReducer extends 
	Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		private DoubleWritable averageWritable = new DoubleWritable();		
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			double sum = 0.0;
			double count = 0;
			for (DoubleWritable val : values) {
				sum += val.get();
				count = count + 1;
			}
			double average = sum/count;
			averageWritable.set(average);
			context.write(key, averageWritable);
		}
		
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new TemperatureAverage(), args);

		System.exit(res);
		

	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "Temperature");
		job.setJarByClass(TemperatureAverage.class);

		job.setMapperClass(TemperatureAverageMapper.class);
		job.setReducerClass(TemperatureAverageReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(getConf());
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

//		job.setNumReduceTasks(2);
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
