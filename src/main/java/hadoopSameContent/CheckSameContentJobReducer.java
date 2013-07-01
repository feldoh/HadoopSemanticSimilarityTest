package hadoopSameContent;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CheckSameContentJobReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, List<String>> dataOriginMap = new HashMap<String, List<String>>();
        
        String[] fullValue;
        long lineTot = 0;
        for (Text val : values) {
            context.getCounter(HadoopCountersEnum.PROCESSED_LINES).increment(1);
            lineTot++;
            fullValue = val.toString().split("\\t", 2);
            List<String> jsonLines = dataOriginMap.get(new File(fullValue[0]).getParent());
            if (jsonLines == null)
                dataOriginMap.put(new File(fullValue[0]).getParent(), jsonLines = new ArrayList<String>());
            jsonLines.add(val.toString());
        }
        
        boolean semanticallyIdentical = true;
        int lastCount = 0;
        
        for (Entry<String, List<String>> entry : dataOriginMap.entrySet()){
            if (lastCount == 0){
                lastCount = entry.getValue().size();
            }else{
                if (lastCount != entry.getValue().size()){
                    semanticallyIdentical = false;
                    break;
                }
            }
        }
        
        if (!semanticallyIdentical){
            String output = null;
            for (Entry<String, List<String>> entry : dataOriginMap.entrySet()){
                String listOfJsonLines = null;
                context.getCounter(HadoopCountersEnum.ERROR_LINES).increment(lineTot);
                for (String json : entry.getValue()){
                    if (listOfJsonLines == null){
                        listOfJsonLines = json;
                    }else{
                        listOfJsonLines = "\t" + json;
                    }
                }
                output = "(" + String.valueOf(entry.getValue().size()) + ")\t" + listOfJsonLines;
            }
            System.err.println(output);
            context.write(key, new Text(output));
        }else{
            context.getCounter(HadoopCountersEnum.CORRECT_LINES).increment(lineTot);
        }
    }
}
