package part2;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

import common.Utils;
import part1.e.AverageComputation;
import part3.Stripes.Partition;

public class Pairs {
	public static class Map extends Mapper<LongWritable, Text, Pair, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] items = value.toString().split(" ");
			List<String>[] neighbors = Utils.getNeighbors(items);

			for (int i = 0; i < items.length; i++) {
				for (int j = 0; j < neighbors[i].size(); j++) {
					context.write(new Pair(items[i], neighbors[i].get(j)), new IntWritable(1));
					context.write(new Pair(items[i], "*"), new IntWritable(1));
				}
			}
		}
	}

	public static class Reduce extends Reducer<Pair, IntWritable, Pair, Double> {

		HashMap<String, Integer> map;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			map = new HashMap<String, Integer>();
		}

		@Override
		protected void reduce(Pair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable i : values)
				sum += i.get();

			if (key.item2.equals("*"))
				map.put(key.item1, sum);
			else
				context.write(key, sum * 1D / map.get(key.item1));

		}
	}

	public static class Partition extends Partitioner<Pair, IntWritable> {

		@Override
		public int getPartition(Pair arg0, IntWritable arg1, int numOfReducer) {
			return Math.abs(arg0.hashCode()) % numOfReducer;
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "averageComputation");

		job.setJarByClass(AverageComputation.class);

		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Pair.class);
		job.setOutputValueClass(DoubleWritable.class);

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
