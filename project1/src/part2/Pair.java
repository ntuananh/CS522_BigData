package part2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {
	public String item1;
	public String item2;

	public Pair() {}
	public Pair(String item1, String item2) {
		this.item1 = item1;
		this.item2 = item2;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		item1 = arg0.readUTF();
		item2 = arg0.readUTF();

	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(item1);
		arg0.writeUTF(item2);
	}

	@Override
	public int compareTo(Pair o) {
		if (this.item1.compareTo(o.item1) == 0) {
			return this.item2.compareTo(o.item2);
		}
		return this.item1.compareTo(o.item1);
	}

	@Override
	public String toString() {
		return "(" + item1 + ", " + item2 + ")";
	}

}
