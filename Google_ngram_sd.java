import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Google_ngram_SD extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {

		private Text volumes = new Text();
		private final static IntWritable one = new IntWritable(1);
		
		public void configure(JobConf job) {
		}

		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			String[] tokens = value.toString().split("\t"); // mapper input is the tab-separated line
			long vols = 0;

			if (isInteger(tokens[1])) {
				vols = Long.parseLong(tokens[3]);
				volumes.set(Long.toString(vols) + "," + Long.toString(vols*vols) + ",1");

		    	output.collect(one,volumes); // mapper output is (one, [number of volumes, number of volumes squared, 1])
			}
		}
    }

    public static class Combine extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {

    	private Text volumes = new Text();

		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			String[] getStats = new String[3];
			long sum = 0;
			long sumSquares = 0;
			int count = 0;

			while (values.hasNext()) {
				getStats = values.next().toString().split(",");
		    	sum += Long.parseLong(getStats[0]);
		    	sumSquares += Long.parseLong(getStats[1]);
		    	count += Integer.parseInt(getStats[2]);
			}

			volumes.set(Long.toString(sum) + "," + Long.toString(sumSquares) + "," + Integer.toString(count));

		    output.collect(key, volumes); // combiner output is (one, [sum, sumSquares, count])
		}
    }

    public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, DoubleWritable> {
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {

		    String[] getStats = new String[3];

		    long sum = 0;
		    long sumSquares = 0;
		    int count = 0;
		    double mean = 0;
		    double stdev = 0;

		    while (values.hasNext()) {
		    	getStats = values.next().toString().split(",");
		    	sum += Long.parseLong(getStats[0]);
		    	sumSquares += Long.parseLong(getStats[1]);
		    	count += Integer.parseInt(getStats[2]);
			}
			mean = sum / count;
			stdev = Math.sqrt((sumSquares-count*mean*mean) / (count-1));

		    output.collect(key, new DoubleWritable(stdev)); // reducer output is (one, stdev volumes)
		}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), Google_ngram_SD.class);
	conf.setJobName("Google_ngram_SD");

	conf.setMapOutputValueClass(Text.class);
	conf.setOutputKeyClass(IntWritable.class);
	conf.setOutputValueClass(DoubleWritable.class);

	conf.setMapperClass(Map.class);
	conf.setCombinerClass(Combine.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new Google_ngram_SD(), args);
	System.exit(res);
    }

    public static boolean isInteger(String s) {
	    try { 
	    	Integer.parseInt(s); 
	    } catch(NumberFormatException e) { 
	    	return false; 
	    }
	    return true;
	}
}
