package hadoopSameContent;

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

	// Initialize job
	Job job = new Job();
	job.setJarByClass(CheckSameContentJob.class);

	// Set input and output paths from command-line parameters
	FileInputFormat.addInputPaths(job, args[0]);
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	// Set Map and Reduce classes
	job.setMapperClass(CheckSameContentJobMapper.class);
	job.setReducerClass(CheckSameContentJobReducer.class);

	// Set output format
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	// JobName must end in :# where # is the number of input files
	// This is to let reducers discover the number of files.
	job.setJobName("Test Content Similarity:" + FileInputFormat.getInputPaths(job).length);

	// Execute job
	boolean result = job.waitForCompletion(true);
	System.exit(result ? 0 : 1);
    }

}
