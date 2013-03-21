package hadoopSameContent;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CheckSameContentJob {

    public static void main(String[] args) throws Exception {
	/*
	 * if (args.length != 3) { System.err.println(
	 * "Usage: CheckSameContentJob <input path1> <input path2> <output path>"
	 * ); System.exit(-1); }
	 */
	Job job = new Job();
	job.setJarByClass(CheckSameContentJob.class);
	job.setJobName("Test Content Similarity");
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileInputFormat.addInputPath(job, new Path(args[1]));
	// FileInputFormat.addInputPath(job, new Path("/tmp/events1.log"));
	// FileInputFormat.addInputPath(job, new Path("/tmp/events2.log"));
	// FileOutputFormat.setOutputPath(job, new Path("/tmp/hadoopOut"));

	FileOutputFormat.setOutputPath(job, new Path(args[2]));
	job.setMapperClass(CheckSameContentJobMapper.class);
	job.setReducerClass(CheckSameContentJobReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	// Make Perl Tester
	StringBuilder files = new StringBuilder();
	for (Path p : FileInputFormat.getInputPaths(job)) {
	    if (files.length() != 0) {
		files.append("|");
	    }
	    String tmp = p.toString().replace(":", "\\:");
	    tmp = tmp.replace("/", "\\/");
	    tmp = tmp.replace("\\", "\\\\");
	    files.append(tmp);
	}

	StringBuilder b = new StringBuilder();
	b.append("perl -ne '!/[a-f 0-9]{40}\\t(");
	b.append(files);
	b.append(")\\,([0-9]+)\\~(");
	b.append(files);
	b.append(")\\,\\2\\~/ && print && print \"\\n\"'");
	boolean result = job.waitForCompletion(true);
	// System.out.println("Correct: " +
	// String.valueOf(job.getCounters().findCounter(HadoopCountersEnum.CORRECT_LINES).getValue()));
	// System.out.println("Inorrect: " +
	// String.valueOf(job.getCounters().findCounter(HadoopCountersEnum.ERROR_LINES).getValue()));
	// System.out.println("Total: " +
	// String.valueOf(job.getCounters().findCounter(HadoopCountersEnum.PROCESSED_LINES).getValue()));
	System.exit(result ? 0 : 1);
    }

}
