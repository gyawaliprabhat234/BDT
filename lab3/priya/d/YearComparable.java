package first.lab3.priya.d;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class YearComparable implements WritableComparable<YearComparable> {

	private int year;

	public YearComparable() {
	}

	public YearComparable(int year) {
		super();
		this.year = year;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		year = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(year);
	}

	@Override
	public int compareTo(YearComparable o) {
		// TODO Auto-generated method stub
		int yr = o.getYear();
		return (yr < year ? -1 : yr == year ? 0 : 1);
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return Integer.toString(year);
	}
	
}
