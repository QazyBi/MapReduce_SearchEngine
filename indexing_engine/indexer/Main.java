//package indexing_engine.indexer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map;

public class Main
{
    public static Map<Text, IntWritable> tf(ArrayList<Text> keys)
    {
        Map<Text, IntWritable> map = new HashMap<Text, IntWritable>();
        ArrayList<Text> uniqueKeys;
        int count = 0;
        uniqueKeys = getUniqueKeys(keys);
        for(Text key: uniqueKeys)
        {
//            if(null == key)
//            {
//                break;
//            }
            for(Text s : keys)
            {
                if(key.equals(s))
                {
                    count++;
                }
            }
            map.put(key, new IntWritable(count));
            count=0;
        }
        return map;
    }

    public static ArrayList<Text> getUniqueKeys(ArrayList<Text> keys)
    {
        ArrayList<Text> uniqueKeys = new ArrayList<Text>(keys.size());
        uniqueKeys.set(0, keys.get(0));
        int uniqueKeyIndex = 1;
        boolean keyAlreadyExists = false;
        for(int i=1; i<keys.size(); i++)
        {
            for(int j=0; j<=uniqueKeyIndex; j++)
            {
                if(keys.get(i).equals(uniqueKeys.get(j)))
                {
                    keyAlreadyExists = true;
                }
            }
            if(!keyAlreadyExists)
            {
                uniqueKeys.set(uniqueKeyIndex, keys.get(i));
                uniqueKeyIndex++;
            }
            keyAlreadyExists = false;
        }
        return uniqueKeys;
    }

    public static ArrayList<FloatWritable> make_vector(Map<Text, IntWritable> map, String[] dcTextLines ){
        Integer len = dcTextLines.length;
        ArrayList<FloatWritable> vector = new ArrayList<FloatWritable>(len);
        for (int i=0; i<len; i++){
            String[] dcElements = dcTextLines[i].split("\t");
            Integer tf = map.get(dcElements[0]).get();
            Integer idf = Integer.parseInt(dcElements[1]);
            FloatWritable ratio = new FloatWritable((float)tf / idf);
            vector.set(i, ratio);
        }
        return vector;
    }
    public static void main(String[] args) {}
}
