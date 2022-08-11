package first.lab3.prabhat.d;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
static class TemperatureWritable implements Writable {
		
		private double sum;
		private double count;

		public TemperatureWritable() {
		}

		public TemperatureWritable(double sum, double count) {
			super();
			this.sum = sum;
			this.count = count;
		}

		public double getSum() {
			return sum;
		}

		public void setSum(double sum) {
			this.sum = sum;
		}

		public double getCount() {
			return count;
		}

		public void setCount(double count) {
			this.count = count;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			sum = in.readDouble();
			count=in.readDouble();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeDouble(sum);
			out.writeDouble(count);
		}
		
	}

	
	
	public static class AverageTemperatureMapper extends 
	Mapper<LongWritable, Text, YearComparable, TemperatureWritable>{		
		private Map<String, TemperatureWritable> map;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			map = new HashMap<>();
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String dateString = value.toString().substring(15, 19);
			String temperatureString = value.toString().substring(87, 92);
			Double temperatureDouble = Double.parseDouble(temperatureString)/10.0;
			double count = 1;
			if(map.containsKey(dateString)){
				TemperatureWritable previousVal = map.get(dateString);
				temperatureDouble += previousVal.getSum();
				count += previousVal.getCount();
				
			}
			map.put(dateString, new TemperatureWritable(temperatureDouble, count));
			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Set<String> yearSet = map.keySet();
			for (String year: yearSet)
			{
				context.write(new YearComparable(Integer.parseInt(year)), map.get(year));
			}
		}
		
	}
	
	
	public static class AverageTemperatureReducer extends 
	Reducer<YearComparable, TemperatureWritable, YearComparable, DoubleWritable>{
		private DoubleWritable result = new DoubleWritable();		
		public void reduce(YearComparable key, Iterable<TemperatureWritable> values,
				Context context) throws IOException, InterruptedException {
			double sum = 0.0;
			double count = 0;
			for (TemperatureWritable val : values) {
				sum += val.getSum();
				count = count + val.getCount();
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

		job.setOutputKeyClass(YearComparable.class);
		job.setOutputValueClass(TemperatureWritable.class);

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
