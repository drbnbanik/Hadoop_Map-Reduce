import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Google_ngram extends Configured implements Tool {

    public static class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

		private DoubleWritable volumes = new DoubleWritable();
		private Text yearString = new Text();

		public void configure(JobConf job) {
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			String[] tokens = value.toString().split("\t"); // mapper input is the tab-separated line
			String[] substring = {"nu","die","kla"};

			int i = 0;
			while (i < substring.length) {
				if (tokens[0].contains(substring[i])) {
					if (isInteger(tokens[1])) {
						yearString.set(tokens[1] + " " + substring[i]);
						volumes.set(Double.parseDouble(tokens[3]));
				    	output.collect(yearString,volumes); // mapper output is ([year substring], number of volumes)
					}
		    	}
		    	i++;
			}
		}
    }

    public static class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

		private DoubleWritable volumes = new DoubleWritable();
		private Text yearString = new Text();

		public void configure(JobConf job) {
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			String[] tokens = value.toString().split("\t"); // mapper input is the tab-separated line
			String[] words = tokens[0].split(" "); // consider words separately
			String[] substring = {"nu","die","kla"};

			int i = 0;
			int j = 0;
			while (i < words.length) {
				j = 0;
				while (j < substring.length) {
					if (words[i].contains(substring[j])) {
						if (isInteger(tokens[1])) {
							yearString.set(tokens[1] + " " + substring[j]);
							volumes.set(Double.parseDouble(tokens[3]));
					    	output.collect(yearString,volumes); // mapper output is ([year substring], number of volumes)
						}
					}
					j++;
		    	}
		    	i++;
			}
		}
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
		    double avgVolumes = 0;
		    double sum = 0;
		    int count = 0;
		    while (values.hasNext()) {
		    	sum += values.next().get();
		    	count += 1;
			}
			avgVolumes = sum / count;

		    output.collect(key, new DoubleWritable(avgVolumes)); // reducer output is ([year substring], average volumes)
		}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), Google_ngram.class);
	conf.setJobName("Google_ngram");

	conf.setMapOutputKeyClass(Text.class);
	conf.setMapOutputValueClass(DoubleWritable.class);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(DoubleWritable.class);

 	// conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, Map1.class);
	MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, Map2.class);
	FileOutputFormat.setOutputPath(conf, new Path(args[2]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new Google_ngram(), args);
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
