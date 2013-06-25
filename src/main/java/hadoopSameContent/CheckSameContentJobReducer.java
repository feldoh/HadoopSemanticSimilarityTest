package hadoopSameContent;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CheckSameContentJobReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        byte count = 0;
        Text outJson = null;
        
        context.getCounter(HadoopCountersEnum.PROCESSED_LINES).increment(1);

        for (Text val : values) {
            if (outJson == null){
                outJson = val;
            }
            count++;
        }
        
        if (count == 2) { // Match Found
            context.getCounter(HadoopCountersEnum.CORRECT_LINES).increment(1);
        } else {
            context.getCounter(HadoopCountersEnum.ERROR_LINES).increment(1);
            if (outJson == null){
                outJson = new Text("");
            }
            context.write(key, outJson);
            return;
        }
    }
}
