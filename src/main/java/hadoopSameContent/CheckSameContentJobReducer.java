package hadoopSameContent;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CheckSameContentJobReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        byte count = 0;
        
        context.getCounter(HadoopCountersEnum.PROCESSED_LINES).increment(1);

        String output = null;
        for (Text val : values) {
            if (output == null){
                output = val.toString();
            } else {
                output += "\t" + val.toString();
            }
            count++;
        }
        
        output = String.valueOf(count) + "\t" + output; 
        
        if (count == 2) { // Match Found
            context.getCounter(HadoopCountersEnum.CORRECT_LINES).increment(1);
        } else {
            context.getCounter(HadoopCountersEnum.ERROR_LINES).increment(1);
            context.write(key, new Text(output));
            System.err.println(output);
            return;
        }
    }
}
