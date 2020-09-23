import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.io.InputStream;
import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;


public class Vocabulary{

     public static void main(String []args) throws IOException {
	// declare configuration
	Configuration configuration = new Configuration();
	FileSystem fs = FileSystem.get(configuration);

	Path pathDocCount = new Path("hdfs://namenode:9000/user/london/output_doc_count/part-r-00000");
	Path pathWordEnum = new Path("hdfs://namenode:9000/user/london/output_word_enum/part-r-00000");

	InputStream inputStreamDC = fs.open(pathDocCount);
	InputStream inputStreamWE = fs.open(pathWordEnum);

	Scanner dc = new Scanner(inputStream).useDelimiter("\\A");
	String dcText = dc.hasNext() ? dc.next() : "";
	String[] lines = dcText.split("\n");
	for (String line : lines){
		String[] elements = line.split("\t");
		System.out.println(elements[0] + " # " + elements[1]);

	}
	fs.close();
     }
}
