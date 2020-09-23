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

import java.util.*;


public class DocCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Pattern p = Pattern.compile("[a-z]+");  // \'?[a-z]+([0-9])*([-'][a-z0-9]+)*\'?
      Set<String> word_set = new HashSet<String>();
      try{
	  JSONObject article_json = new JSONObject(value.toString());
	  Matcher m = p.matcher(article_json.get("text").toString().toLowerCase());
	  while(m.find()) {
		String w = m.group();
	  	word.set(w);
		if(! word_set.contains(w)) {
			context.write(word, one);
			word_set.add(w);
		}
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
      IntWritable result = new IntWritable(1);
      int total = 0;
      for (IntWritable elem : values){
	total += elem.get();
      }
      result.set(total);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "doc count");
    job.setJarByClass(DocCount.class);
    job.setMapperClass(TokenizerMapper.class);
//    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

