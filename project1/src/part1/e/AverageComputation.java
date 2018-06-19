package part1.e;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

import part1.f.AverageComputationCombiner.Partition;

public class AverageComputation {


	public static class Map extends	Mapper<LongWritable, Text, Text, Pair> {
		
		Text ip = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] quantities = line.split(" ");
			try {
				ip.set(quantities[0]);
				context.write(ip, new Pair(Integer.parseInt(quantities[quantities.length-1]), 1));
			} catch (Exception e) {
			}
		}

	}
	
	public static class Reduce extends Reducer<Text, Pair, Text, Double> {

		@Override
		protected void reduce(Text key, Iterable<Pair> values, Context context)	throws IOException, InterruptedException {
			int time = 0;
			int count = 0;
			for(Pair value: values) {
				time += value.time;
				count += value.count;
			}
			context.write(key, time*1D/count);
		}
	}
	
	public static class Partition extends Partitioner<Text, Pair> {

		@Override
		public int getPartition(Text arg0, Pair arg1, int numOfReducer) {
			return Math.abs(arg0.hashCode()) % numOfReducer;
		}
	}
	
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "averageComputation");

        job.setJarByClass(AverageComputation.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Pair.class);
        job.setOutputKeyClass(Text.class);
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
