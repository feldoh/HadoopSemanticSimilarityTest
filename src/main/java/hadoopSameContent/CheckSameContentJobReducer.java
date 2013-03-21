package hadoopSameContent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CheckSameContentJobReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	Map<String, Integer> count = new HashMap<String, Integer>();
	context.getCounter(HadoopCountersEnum.PROCESSED_LINES).increment(1);

	// Iterate over the values to reduce and convert them into a HashMap of
	// <Origin File, Number of Occurrences in File>
	for (Text val : values) {
	    if (count.containsKey(val.toString())) {
		count.put(val.toString(), count.get(val.toString()) + 1);
	    } else {
		count.put(val.toString(), 1);
	    }
	}

	// Iterate over the final HashMap to check that for this line the same
	// number of occurrences appear in all input files
	int testVal = 0; // Test each count against the rest using this variable
	String result = "";
	for (Entry<String, Integer> entry : count.entrySet()) {
	    result += entry.getKey() + "," + String.valueOf(entry.getValue() + "~");
	    if (testVal == 0) {
		testVal = entry.getValue(); // First count (baseline) for the
					    // key
	    } else {
		if (testVal == entry.getValue()) { // Match Found
		    context.getCounter(HadoopCountersEnum.CORRECT_LINES).increment(1);
		} else {
		    // No Match
		    // Two counts differed so print the pair <Canonical Json,
		    // (String, listing each file the object was found in and
		    // how many times it was found in that file)>
		    context.getCounter(HadoopCountersEnum.ERROR_LINES).increment(1);
		    context.write(key, new Text(result));
		    return;
		}
	    }
	}

	// Special Case for when there is a row in one or more files which does
	// not appear in at least one other file. This is done by testing the
	// cardinality of the map against the number of files which is appended
	// to the end of the job name for the purpose of this test.
	if (!(context.getJobName().endsWith(":" + String.valueOf(count.size())))) {
	    context.getCounter(HadoopCountersEnum.ERROR_LINES).increment(1);
	    context.write(key, new Text(result));
	}
    }
}
