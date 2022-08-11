package first;

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

public class Temperature extends Configured implements Tool{
	
	public static class AverageTemperatureMapper extends 
	Mapper<LongWritable, Text, Text, DoubleWritable>{
		private Text date = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String dateString = value.toString().substring(15, 19);
			String temperatureString = value.toString().substring(87, 92);
			Double temperatureDouble = Double.parseDouble(temperatureString)/10.0;
			date.set(dateString);
			DoubleWritable temperature = new DoubleWritable(temperatureDouble);
//			temperature.set();
			context.write(date, temperature);
			
		}
		
	}
	
	public static class AverageTemperatureReducer extends 
	Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		private DoubleWritable result = new DoubleWritable();		
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			double sum = 0.0;
			double count = 0;
			for (DoubleWritable val : values) {
				sum += val.get();
				count = count + 1;
			}
			result.set(sum/count);
			context.write(key, result);
		}
		
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new Temperature(), args);

		System.exit(res);
		

	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "Temperature");
		job.setJarByClass(Temperature.class);

		job.setMapperClass(AverageTemperatureMapper.class);
		job.setReducerClass(AverageTemperatureReducer.class);

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
