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
            if(null == key)
            {
                break;
            }
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
	if (keys == null  || keys.isEmpty()){
		return uniqueKeys;
	}else{
	     try{
        	uniqueKeys.set(0, keys.get(0));
	     }
	     catch (IndexOutOfBoundsException e){
		return uniqueKeys;
	     }
        	int uniqueKeyIndex = 1;
        	boolean keyAlreadyExists = false;
        	for(int i=1; i<keys.size(); i++)
	        {
        	    for(int j=0; j<uniqueKeyIndex; j++)
    		    {
                	if(keys.get(i).equals(uniqueKeys.get(j)))
                	{
                    		keyAlreadyExists = true;
		    		break;
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
    }

    public static ArrayList<FloatWritable> make_vector(Map<Text, IntWritable> map, String[] dcTextLines ){
	try { 
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
	}catch(NullPointerException e){
		return new ArrayList<FloatWritable>(0);
	}
    }

	public static Map<IntWritable, FloatWritable> inner_product(String[] IndTextLines, ArrayList<FloatWritable> query_vector){
        Map<IntWritable, FloatWritable> relevance_map = new HashMap<IntWritable, FloatWritable>();
        Integer len = IndTextLines.length;
        for (int i=0; i<len; i++){
            String[] dcElements = dcTextLines[i].split("\t");
            ArrayList<FloatWritable> vector = map.get(dcElements[1]);
            FloatWritable relevance = relevance(vector, query_vector);
            relevance_map.put(dcElements[0], relevance);
        }
        return relevance_map;
    }

    public static FloatWritable relevance(ArrayList<FloatWritable> arr1, ArrayList<FloatWritable> arr2){
        Integer len = arr1.size();
        for (int i=0;i<len;i++){
            FloatWritable p = arr1.get(i)*arr2.get(i);
            arr1.set(i, p);
        }
        Integer sum = 0;
        for (int i=0;i<len;i++){
            sum = sum + arr1.get(i);
        }
        return sum
    }

    public static void main(String[] args) {}
}
