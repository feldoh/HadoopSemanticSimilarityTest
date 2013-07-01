package hadoopSameContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CheckSameContentJobMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    /*
     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
     * org.apache.hadoop.mapreduce.Mapper.Context)
     * 
     * Convert the input to a canonical JSON string if possible Retrieve the
     * input file this data came from Return the pair {Canonical Data, file
     * origin}
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        JsonLine id = (getIdentifyingRequestHashFromJson(value.toString(), context));
        
        if (id != null) {
            if(id.hasFoundEventType()){
                if (id.getIntermediateOutput() != null){
                    context.write(new Text(id.getVdna_widget_mc()), new Text(id.getIntermediateOutput()));
                }else{
                    context.getCounter(HadoopCountersEnum.INVALID_LINES).increment(1);
                }
            } else {
                context.getCounter(HadoopCountersEnum.NON_TRACKED_EVENT).increment(1);
            }
        }
    }

    /*
     * Process the JSON node provided, loading it into a TreeMap to provide a
     * consistent order. If nested objects are encountered they will be
     * processed recursively to independently canonicalize the nested object
     * contents giving a fully canonical form.
     * 
     * TODO: Does not currently handle JSON Array types
     */
    private JsonLine getIdentifyingRequestHashFromJson(JsonNode rootNode, JsonLine id) {

        // Iterate over all nodes at this level of the JSON Tree
        Iterator<Entry<String, JsonNode>> fields = rootNode.getFields();
        while (fields.hasNext()) {
            Entry<String, JsonNode> field = fields.next();

            // Handle recursion cases
            if (field.getValue().isObject()) {
                // Detected a nested object, so make a recursive call to
                // determine the object's canonical form before storing it as a
                // string at this level of the tree.
                id = getIdentifyingRequestHashFromJson(field.getValue(), id);
            } else {
                // Base case
                if (field.getKey().equals("event_type")){
                    id.setFoundEventType();
                } else if (field.getKey().toLowerCase().equals("vdna_widget_mc")){
                    if (field.getValue().toString().toLowerCase().length() == 36){
                        id.setVdna_widget_mc(field.getValue().toString().toLowerCase());
                    }else{
                        return null;
                    }
                } else{
                    continue;
                }
            }
        }

        return id;
    }

    /*
     * Top level wrapper for:
     * 
     * @see getIdentifyingRequestHashFromJson(JsonNode)
     * 
     * Uses the context from the map task to update JSON or TEXT counters
     * respectively and parse the initial string into a root JSON object if
     * possible. This allows the main canonicalization function to remain
     * decoupled from this class for potential re-use. If it can be parsed it
     * will pass the generated JsonNode object to the canonicaliseJson(JsonNode)
     * method for canonicalization.
     */
    private JsonLine getIdentifyingRequestHashFromJson(String from, Context context) throws JsonParseException, IOException {
        JsonFactory factory = new JsonFactory();
        ObjectMapper mapper = new ObjectMapper(factory);
        JsonParser jp;
        jp = factory.createJsonParser(from);
        JsonNode rootNode;

        try {
            rootNode = mapper.readTree(jp); // Attempt to parse into Json.

            // If this point is reached then the data is Json so return its
            // canonical form.
            context.getCounter(HadoopCountersEnum.JSON_LINES).increment(1);
            JsonLine id = new JsonLine(from, ((FileSplit) context.getInputSplit()).getPath().toString());
            id = getIdentifyingRequestHashFromJson(rootNode, id);
            return id;
        } catch (JsonProcessingException e) {
            // If it could not read a JSON Structure then treat as a string.
            context.getCounter(HadoopCountersEnum.TEXT_LINES).increment(1);
            // return from;
            return null; // If not JSON ignore the line
        }
    }
}
