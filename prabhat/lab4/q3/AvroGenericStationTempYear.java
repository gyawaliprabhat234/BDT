package first.prabhat.lab4.q3;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import first.prabhat.lab4.q2.NcdcLineReaderUtils;

public class AvroGenericStationTempYear extends Configured implements Tool
{

	private static Schema MAPPER;
	private static Schema REDUCER;

	public static class AvroMapper extends Mapper<LongWritable, Text, AvroKey<GenericRecord>, AvroValue<String>>
	{
		private NcdcLineReaderUtils utils = new NcdcLineReaderUtils();
		private GenericRecord record = new GenericData.Record(MAPPER);
		AvroKey<GenericRecord> avroKey = new AvroKey<GenericRecord>(record);
		AvroValue<String> avroValue = new AvroValue<String>("");

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			utils.parse(value.toString());

			if (utils.isValidTemperature())
			{
				record.put("temperature", utils.getAirTemperature());
				record.put("year", utils.getYearInt());
				
				//populate the avroKey with record
				avroKey.datum(record);
				avroValue.datum(utils.getStationId());
				
				context.write(avroKey, avroValue);
			}
		}
	}

	public static class AvroReducer extends Reducer<AvroKey<GenericRecord>, AvroValue<String>, AvroKey<GenericRecord>, NullWritable>{
		
		private boolean isCurrentYear = false;
		private int thisYear = 0;
		private GenericRecord record = new GenericData.Record(REDUCER);
		AvroKey<GenericRecord> avroKey = new AvroKey<GenericRecord>(record);

		@Override
		protected void reduce(AvroKey<GenericRecord> key, Iterable<AvroValue<String>> values,
				Context context) throws IOException, InterruptedException
		{
			GenericRecord recordMapper = key.datum();
			int year = Integer.parseInt(recordMapper.get("year").toString());
			
			if (isCurrentYear) {
				if (year == thisYear)
					return;
			} else {
				isCurrentYear = true;
			}
			thisYear = year;
			record.put("year", year);
			record.put("temperature", recordMapper.get("temperature"));
			
			String stationId="";
			Iterator<AvroValue<String>> iterator = values.iterator();
			while(iterator.hasNext()) {
				stationId+=iterator.next().datum();
				if(iterator.hasNext())
					stationId+=", ";
			}
			record.put("stationId", stationId);
			avroKey.datum(record);		
			context.write(avroKey, NullWritable.get());
		}
	}

	@Override
	public int run(String[] args) throws Exception
	{
		if (args.length != 4)
		{
			System.err.printf("Usage: %s [generic options] <input> <output> <schema-file>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Job job = Job.getInstance();
		job.setJarByClass(AvroGenericStationTempYear.class);
		job.setJobName("Avro Station-Temp-Year");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		String mapperSchemaFile = args[2];
		String reducerSchemaFile = args[3];
		System.out.println(mapperSchemaFile);
		System.out.println(reducerSchemaFile);
		

		MAPPER = new Schema.Parser().parse(new File(mapperSchemaFile));

		REDUCER = new Schema.Parser().parse(new File(reducerSchemaFile));

		job.setMapperClass(AvroMapper.class);
		job.setReducerClass(AvroReducer.class);
		
		// This is important
		job.setMapOutputValueClass(NullWritable.class);
		

		AvroJob.setMapOutputKeySchema(job, MAPPER);  
		AvroJob.setMapOutputValueSchema(job, Schema.create(Schema.Type.STRING)); 
		
		// Need to set output key schema as follows:
		AvroJob.setOutputKeySchema(job, REDUCER);

		
		job.setOutputFormatClass(AvroKeyOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		FileSystem.get(conf).delete(new Path("output"), true);
		int res = ToolRunner.run(conf, new AvroGenericStationTempYear(), args);		
		System.exit(res);
	}
}