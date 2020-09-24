import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.ArrayList;


public class Vocabulary{

     public static void main(String []args) throws IOException {
	// declare configuration
	Configuration configuration = new Configuration();
	FileSystem fs = FileSystem.get(configuration);
	String dir = "hdfs://namenode:9000/user/london/output_voc/";
	String file = "part-r-00000";
	Path pathDocCount = new Path("hdfs://namenode:9000/user/london/output_doc_count/part-r-00000");
	Path pathWordEnum = new Path("hdfs://namenode:9000/user/london/output_word_enum/part-r-00000");
	Path pathOutputDir = new Path(dir);
	Path pathOutputFile = new Path(dir + file);

	// create a directory to store a file
	fs.mkdirs(pathOutputDir);

	// initialize input and output stream
	FSDataOutputStream outputStream = fs.create(pathOutputFile);
	InputStream inputStreamDC = fs.open(pathDocCount);
	InputStream inputStreamWE = fs.open(pathWordEnum);

	Scanner dc = new Scanner(inputStreamDC).useDelimiter("\\A");
	String dcText = dc.hasNext() ? dc.next() : "";
	String[] dcTextLines = dcText.split("\n");

	Scanner we = new Scanner(inputStreamWE).useDelimiter("\\A");
	String weText = we.hasNext() ? we.next() : "";
	String[] weTextLines = weText.split("\n");


	for (int i=0; i<weTextLines.length; i++){
		String line = "";
		String[] weElements = weTextLines[i].split("\t");
		String[] dcElements = dcTextLines[i].split("\t");
		line = weElements[0] + "\t" + dcElements[1] + "\t" + weElements[1] + "\n";
		outputStream.writeBytes(line);
	}
	fs.close();
	outputStream.close();
     }
}
