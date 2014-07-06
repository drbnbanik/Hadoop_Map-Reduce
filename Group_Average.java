import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Group_Average extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

	private Text finalKey = new Text();
	private DoubleWritable col4 = new DoubleWritable();

	public void configure(JobConf job) {
	}

	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	    String line = value.toString();
	    String[] localArray = line.split(",");
	    int length = localArray.length;
	    StringBuilder sb = new StringBuilder();

	    if(localArray[length-1].equals("false")){

			sb.append(localArray[29]).append(",").append(localArray[30]).append(",").append(localArray[31]).append(",").append(localArray[32]);
	    	finalKey.set(sb.toString());
	    	col4.set(Double.parseDouble(localArray[3]));
	    	output.collect(finalKey,col4);
	    }

    }
}

    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	    Double sum = 0.0;
	    Double count = 0.0;
	    Double average = 0.0;
	    while (values.hasNext()) {
	    	sum += values.next().get();
	    	count += 1.0;
	    }
	    average = sum/count;
	    output.collect(key, new DoubleWritable(average));
	}
}

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), Group_Average.class);
	conf.setJobName("Group_Average");

	conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(DoubleWritable.class);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(DoubleWritable.class);

	conf.setMapperClass(Map.class);
	conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new Group_Average(), args);
	System.exit(res);
    }
}
