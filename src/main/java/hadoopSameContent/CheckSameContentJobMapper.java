package hadoopSameContent;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CheckSameContentJobMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	try {
	    String hash = SHA1(value.toString());
	    String filename = ((FileSplit) context.getInputSplit()).getPath().toString();
	    context.write(new Text(hash), new Text(filename));
	} catch (NoSuchAlgorithmException e) {
	    // TODO: improve error trapping
	    e.printStackTrace();
	}
    }

    public static String SHA1(String text) throws NoSuchAlgorithmException, UnsupportedEncodingException {
	MessageDigest md;
	md = MessageDigest.getInstance("SHA-1");
	byte[] sha1hash = new byte[40];
	md.update(text.getBytes("iso-8859-1"), 0, text.length());
	sha1hash = md.digest();
	return convToHex(sha1hash);
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