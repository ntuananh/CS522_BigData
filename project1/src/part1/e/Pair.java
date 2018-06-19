package part1.e;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Pair implements Writable {
	public int time;
	public int count;

	public Pair() {
	}

	public Pair(int time, int count) {
		this.time = time;
		this.count = count;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		time = arg0.readInt();
		count = arg0.readInt();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(time);
		arg0.writeInt(count);
	}
}