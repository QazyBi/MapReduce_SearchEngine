//package indexing_engine.indexer;

import java.io.IOException;
import java.util.StringTokenizer;
import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.InputStream;
import org.apache.hadoop.fs.FileSystem;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

// libraries for JSON deserialization
import org.json.JSONObject;
import org.json.JSONException;
// libraries for filtering out only words from strings
import java.util.regex.Matcher;
import java.util.regex.Pattern;
// library for random
import java.util.Random;
import java.io.*;
import java.util.*;

public class Indexer {

    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, Text>{

//        private final static IntWritable one = new IntWritable(1);
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
                        context.write(new IntWritable(Integer.parseInt(article_json.get("id").toString())), word);
                        word_set.add(w);
                    }
                }
            }catch(JSONException e){
                e.printStackTrace();
            }
        }
    }
    public static class IntReduce
            extends Reducer<IntWritable,Text,IntWritable,ArrayList> {
        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Main instance = new Main();
            Map<Text, IntWritable> map = new HashMap<Text, IntWritable>();
            map = instance.tf(Lists.newArrayList(values));

            ////
            Configuration configuration = new Configuration();
            FileSystem fs = FileSystem.get(configuration);
            Path pathDocCount = new Path("hdfs://namenode:9000/user/london/output_doc_count/part-r-00000");
            InputStream inputStreamDC = fs.open(pathDocCount);
            Scanner dc = new Scanner(inputStreamDC).useDelimiter("\\A");
            String dcText = dc.hasNext() ? dc.next() : "";
            String[] dcTextLines  = dcText.split("\n");
            ///

            ArrayList<FloatWritable> vector = instance.make_vector(map, dcTextLines);



//            IntWritable result = new IntWritable(1);
//            int total = 0;
//            for (IntWritable elem : values){
//                total += elem.get();
//            }
//            result.set(total);
            context.write(key, vector);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "doc count");
        job.setJarByClass(Indexer.class);
        job.setMapperClass(TokenizerMapper.class);
//    job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
