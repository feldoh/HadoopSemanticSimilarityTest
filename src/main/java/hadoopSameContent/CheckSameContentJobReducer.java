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
	for (Text val : values) {
	    if (count.containsKey(val.toString())) {
		count.put(val.toString(), count.get(val.toString()) + 1);
	    } else {
		count.put(val.toString(), 1);
	    }
	}

	int testVal = 0;
	String result = "";
	for (Entry<String, Integer> entry : count.entrySet()) {
	    result += entry.getKey() + "," + String.valueOf(entry.getValue() + "~");
	    if (testVal == 0) {
		testVal = entry.getValue();
	    } else {
		if (testVal == entry.getValue()) {
		    context.getCounter(HadoopCountersEnum.CORRECT_LINES).increment(1);
		} else {
		    context.getCounter(HadoopCountersEnum.ERROR_LINES).increment(1);
		    context.write(key, new Text(result));
		}
	    }
	}

	context.getCounter(HadoopCountersEnum.PROCESSED_LINES).increment(1);
    }
}
