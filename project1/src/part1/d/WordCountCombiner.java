package part1.d;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import part1.e.Pair;
import part1.f.AverageComputationCombiner.Partition;

public class WordCountCombiner {

	public static class Map extends	Mapper<LongWritable, Text, Text, IntWritable> {
		private Text word = new Text();
		private HashMap<String, Integer> map;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			map = new HashMap<>();
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			int count;
            for(String w : line.split("\\W+")) {
            	word.set(w);
            	count = map.get(word) == null ? 1 : map.get(word) + 1;
            	context.write(word, new IntWritable(count));
            }
		}

		@Override
		protected void cleanup(Context context)	throws IOException, InterruptedException {

			for (Entry<String, Integer> entry : map.entrySet()) {
				word.set(entry.getKey());
				context.write(word, new IntWritable(entry.getValue()));
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static class Partition extends Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text arg0, IntWritable arg1, int numOfReducer) {
			return Math.abs(arg0.hashCode()) % numOfReducer;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "wordcountCombiner");

		job.setJarByClass(WordCountCombiner.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

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