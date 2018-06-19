package part3;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import common.Stripe;
import common.Utils;

import part1.e.AverageComputation;

public class Stripes {
	public static class Map extends Mapper<LongWritable, Text, Text, Stripe> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] items = value.toString().split(" ");
			List<String>[] neighbors = Utils.getNeighbors(items);

			for (int u = 0; u < items.length; u++) {
				Stripe stripe = new Stripe();

				for (int w = 0; w < neighbors[u].size(); w++) {
					Text keyStripe = new Text(neighbors[u].get(w));
					IntWritable valWritable = (IntWritable) stripe.get(keyStripe);
					int valStripe = valWritable == null ? 1	: valWritable.get() + 1;
					stripe.put(keyStripe, new IntWritable(valStripe));
				}
				context.write(new Text(items[u]), stripe);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Stripe, Text, Stripe> {

		@Override
		protected void reduce(Text key, Iterable<Stripe> values, Context context)
				throws IOException, InterruptedException {

			Stripe stripeF = new Stripe();

			for (Stripe stripe : values) {
				stripeF.plus(stripe);
			}

			if(!stripeF.isEmpty())
				context.write(key, stripeF.normalize());
		}
	}

	public static class Partition extends Partitioner<Text, MapWritable> {

		@Override
		public int getPartition(Text arg0, MapWritable arg1, int numOfReducer) {
			return Math.abs(arg0.hashCode()) % numOfReducer;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "Stripes");

		job.setJarByClass(AverageComputation.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Stripe.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setPartitionerClass(Partition.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileUtils.deleteDirectory(new File(args[1]));

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
