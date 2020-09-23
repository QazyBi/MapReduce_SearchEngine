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

	Scanner dc = new Scanner(inputStreamDC).useDelimiter("\\A");
	String dcText = dc.hasNext() ? dc.next() : "";
	String[] dcTextLines = dcText.split("\n");

	Scanner we = new Scanner(inputStreamWE).useDelimiter("\\A");
	String weText = we.hasNext() ? we.next() : "";
	String[] weTextLines = weText.split("\n");

	HashMap<String, ArrayList<Integer>> hmap = new HashMap<String, ArrayList<Integer>>();

	for (int i=0; i<weTextLines.length; i++){
		String[] weElements = weTextLines[i].split("\t");
		String[] dcElements = dcTextLines[i].split("\t");
		ArrayList<Integer> nums = new ArrayList<Integer>();

		nums.add(dcElements[1]);
		nums.add(weElements[1]);
		hmap.put(weElements[0], nums);
	}
	fs.close();
     }
}
