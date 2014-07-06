import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Yearly_Max_Temp extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	private Text year = new Text();
	private IntWritable temp = new IntWritable();

	public void configure(JobConf job) {
	}

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	    String line = value.toString();
		year.set(line.substring(15,19));
		String sign = line.substring(87,88);
		int temperature = Integer.parseInt(line.substring(88,92));
		String flag = line.substring(88,92);
	    String temp_quality = line.substring(92,93);

	    if(sign.equals("-")){
	    	temperature = temperature * (-1);
	    }

	    temp.set(temperature);

	    if((temp_quality.equals("0"))||(temp_quality.equals("1"))||(temp_quality.equals("4"))||(temp_quality.equals("5"))||(temp_quality.equals("9"))){
	    	if(!flag.equals("9999")){
	    		output.collect(year,temp);
	    	}
	    	
	    }
		
    }
}

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	    int max = -99999;
	    while (values.hasNext()) {
	    	int temp = values.next().get();
	    	if(temp > max){
	    		max = temp;
	    	}
	    }
	    output.collect(key, new IntWritable(max));
	}
}

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), Yearly_Max_Temp.class);
	conf.setJobName("Yearly_Max_Temp");

	conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(IntWritable.class);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);

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
	int res = ToolRunner.run(new Configuration(), new Yearly_Max_Temp(), args);
	System.exit(res);
    }
}
