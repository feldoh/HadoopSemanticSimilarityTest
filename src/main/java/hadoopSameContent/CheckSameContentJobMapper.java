package hadoopSameContent;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CheckSameContentJobMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	String hash = (value.toString());
	String filename = ((FileSplit) context.getInputSplit()).getPath().toString();
	context.write(new Text(hash), new Text(filename));
    }
}