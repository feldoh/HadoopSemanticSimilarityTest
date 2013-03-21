package hadoopSameContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

public class CheckSameContentJobMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	String hash = (canonicaliseJson(value.toString(), context));
	String filename = ((FileSplit) context.getInputSplit()).getPath().toString();
	context.write(new Text(hash), new Text(filename));
    }

    private String canonicaliseJson(JsonNode rootNode) {
	TreeMap<String, String> outJson = new TreeMap<String, String>();

	Iterator<Entry<String, JsonNode>> fields = rootNode.getFields();
	while (fields.hasNext()) {

	    Entry<String, JsonNode> field = fields.next();

	    if (field.getValue().isObject()) {
		outJson.put(field.getKey(), canonicaliseJson(field.getValue()));
	    } else {
		outJson.put(field.getKey(), field.getValue().toString());
	    }
	}

	StringBuilder retJson = null;
	for (Entry<String, String> e : outJson.entrySet()) {
	    if (retJson == null) {
		retJson = new StringBuilder("{");
	    } else {
		retJson.append(",");
	    }
	    retJson.append("\"" + e.getKey() + "\"").append(":").append(e.getValue());
	}
	retJson.append("}");

	return retJson.toString();
    }

    private String canonicaliseJson(String from, Context context) throws JsonParseException, IOException {
	JsonFactory factory = new JsonFactory();
	ObjectMapper mapper = new ObjectMapper(factory);
	JsonParser jp;
	jp = factory.createJsonParser(from);
	JsonNode rootNode;

	try {
	    rootNode = mapper.readTree(jp);
	    context.getCounter(HadoopCountersEnum.JSON_LINES).increment(1);
	    return canonicaliseJson(rootNode);
	} catch (JsonProcessingException e) {
	    // If it could not read a JSON Structure then treat as a string.
	    context.getCounter(HadoopCountersEnum.TEXT_LINES).increment(1);
	    return from;
	}
    }
}