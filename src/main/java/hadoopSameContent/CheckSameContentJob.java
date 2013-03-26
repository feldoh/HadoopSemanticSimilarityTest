package hadoopSameContent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CheckSameContentJob {

    public static void main(String[] args) throws Exception {

	if (args.length != 2) {
	    System.err.println("Usage: CheckSameContentJob <comma seperated list of input files> <output path>");
	    System.exit(-1);
	}

	// Configure Job Settings
	Configuration conf = new Configuration();
	conf.set("numFiles", String.valueOf(args[0].split(",").length));

	// Initialize job
	Job job = new Job(conf);
	job.setJarByClass(CheckSameContentJob.class);
	job.setJobName("Test Content Similarity");

	// Set input and output paths from command-line parameters
	FileInputFormat.addInputPaths(job, args[0]);
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	// Set Map and Reduce classes
	job.setMapperClass(CheckSameContentJobMapper.class);
	job.setReducerClass(CheckSameContentJobReducer.class);

	// Set output format
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	// Execute job
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
