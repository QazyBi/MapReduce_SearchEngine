import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// libraries for JSON deserialization
import org.json.JSONObject;
import org.json.JSONException;
// libraries for filtering out only words from strings
import java.util.regex.Matcher;
import java.util.regex.Pattern;
// library for random
import java.util.Random;


public class WordEnum {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Pattern p = Pattern.compile("[a-z]+");  // \'?[a-z]+([0-9])*([-'][a-z0-9]+)*\'?
      try{
	  JSONObject article_json = new JSONObject(value.toString());
	  Matcher m = p.matcher(article_json.get("text").toString().toLowerCase());
	  while(m.find()) {
	  	word.set(m.group());
		context.write(word, one);
	  }
      }catch(JSONException e){
	     e.printStackTrace();
      }
   }
}
  public static class IntSumReducer
       extends Reducer<Text, IntWritable,Text,IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      IntWritable id = new IntWritable();
      Random rand = new Random();
      String binaryString = Long.toString(Math.abs(rand.nextLong()), 2).substring(0, 8) + 
				Long.toString(System.currentTimeMillis(),2).substring(25, 35) +
				Long.toString(Math.abs(rand.nextLong()),2).substring(0, 13);
      Integer id_val = Integer.parseInt(binaryString, 2);
      id.set(id_val);
      context.write(key, id);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word enum");
    job.setJarByClass(WordEnum.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

