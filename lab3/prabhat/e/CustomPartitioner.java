package first.lab3.prabhat.e;

import org.apache.hadoop.mapreduce.Partitioner;

import first.lab3.prabhat.e.Temperature.TemperatureWritable;

public class CustomPartitioner extends Partitioner<YearComparable, TemperatureWritable> {

	@Override
	public int getPartition(YearComparable year, TemperatureWritable temp, int numberOfReducer) {
		System.out.println(year + " " + numberOfReducer);
		if (numberOfReducer == 0) return 0;
		if(year.getYear() <= 1930) return 0;
		return 1;
	}

}
