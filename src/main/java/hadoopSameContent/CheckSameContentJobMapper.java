package hadoopSameContent;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
	String json = (canonicaliseJson(value.toString(), context));
	if (json != null) {
	    String filename = ((FileSplit) context.getInputSplit()).getPath().getParent().toString();
	    // System.err.println(filename);
	    context.write(new Text(SHA1(json)), new Text(filename));
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
    private String canonicaliseJson(JsonNode rootNode) {
	TreeMap<String, String> outJson = new TreeMap<String, String>();

	// Iterate over all nodes at this level of the JSON Tree
	Iterator<Entry<String, JsonNode>> fields = rootNode.getFields();
	while (fields.hasNext()) {
	    Entry<String, JsonNode> field = fields.next();

	    // Handle recursion cases
	    if (field.getValue().isObject()) {
		// Detected a nested object, so make a recursive call to
		// determine the object's canonical form before storing it as a
		// string at this level of the tree.
		String nestedObj = canonicaliseJson(field.getValue());
		if (nestedObj == null) {
		    return null;
		}
		outJson.put(field.getKey(), nestedObj);
	    } else {
		// Base case
		if (field.getKey().equals("event_type") && !((field.getValue().toString().equals("esVDNAAppUserActionEvent")) || (field.getValue().toString().equals("\"esVDNAAppUserActionEvent\"")))) {
		    return null;
		}
		outJson.put(field.getKey(), field.getValue().toString());
	    }
	}

	// Iterate over the TreeMap, combining all entries at this node to
	// calculate a single string to return.
	StringBuilder retJson = null;
	for (Entry<String, String> e : outJson.entrySet()) {
	    if (retJson == null) {
		retJson = new StringBuilder("{");
	    } else {
		retJson.append(",");
	    }

	    // Re-add the quotes around the keys and output the pair
	    retJson.append("\"" + e.getKey() + "\"").append(":").append(e.getValue());
	}
	retJson.append("}");

	return retJson.toString();
    }

    /*
     * Top level wrapper for:
     * 
     * @see canonicaliseJson(JsonNode)
     * 
     * Uses the context from the map task to update JSON or TEXT counters
     * respectively and parse the initial string into a root JSON object if
     * possible. This allows the main canonicalization function to remain
     * decoupled from this class for potential re-use. If it can be parsed it
     * will pass the generated JsonNode object to the canonicaliseJson(JsonNode)
     * method for canonicalization.
     */
    private String canonicaliseJson(String from, Context context) throws JsonParseException, IOException {
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
	    return canonicaliseJson(rootNode);
	} catch (JsonProcessingException e) {
	    // If it could not read a JSON Structure then treat as a string.
	    context.getCounter(HadoopCountersEnum.TEXT_LINES).increment(1);
	    // return from;
	    return null; // If not JSON ignore the line
	}
    }

    public static String SHA1(String text) {
	MessageDigest md;
	try {
	    md = MessageDigest.getInstance("SHA-1");
	    byte[] sha1hash = new byte[40];
	    md.update(text.getBytes("iso-8859-1"), 0, text.length());
	    sha1hash = md.digest();
	    return convToHex(sha1hash);
	} catch (NoSuchAlgorithmException e) {
	    e.printStackTrace();
	    return text;
	} catch (UnsupportedEncodingException e) {
	    e.printStackTrace();
	    return text;
	}
    }

    private static String convToHex(byte[] data) {
	StringBuilder buf = new StringBuilder();
	for (int i = 0; i < data.length; i++) {
	    int halfbyte = (data[i] >>> 4) & 0x0F;
	    int two_halfs = 0;
	    do {
		if ((0 <= halfbyte) && (halfbyte <= 9))
		    buf.append((char) ('0' + halfbyte));
		else
		    buf.append((char) ('a' + (halfbyte - 10)));
		halfbyte = data[i] & 0x0F;
	    } while (two_halfs++ < 1);
	}
	return buf.toString();
    }
}
