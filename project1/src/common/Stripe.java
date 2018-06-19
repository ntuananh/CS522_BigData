package common;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Stripe extends MapWritable {

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("{");

		for (Entry<Writable, Writable> entry : entrySet()) {
			sb.append(String.format("%s: %.2f, ", entry.getKey(), ((DoubleWritable)entry.getValue()).get()));
		}
		if (sb.lastIndexOf(", ") != -1) {
			return sb.substring(0, sb.lastIndexOf(", ")) + "}";
		}
		return sb.append("}").toString();
	}

	public Stripe normalize() {
		double sum = 0;
		Stripe map = new Stripe();
		for (Writable val : values()) {
			sum += ((IntWritable) val).get();
		}

		for (Entry<Writable, Writable> entry : entrySet()) {
			double newVal = ((IntWritable) entry.getValue()).get() * 1.0 / sum;
			map.put(entry.getKey(), new DoubleWritable(newVal));
		}
		return map;
	}

	public Stripe plus(Stripe stripe) {
		for (Entry<Writable, Writable> entry : stripe.entrySet()) {
			Writable key = entry.getKey();
			int val = ((IntWritable) entry.getValue()).get();
			if (this.containsKey(key)) {
				val += ((IntWritable) this.get(key)).get();
			}
			this.put(key, new IntWritable(val));
		}
		return this;
	}
}
