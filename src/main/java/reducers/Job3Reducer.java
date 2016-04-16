package reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Job3Reducer
		extends Reducer<Text,IntWritable,Text,IntWritable> {
	
//	private Hashtable<String, Integer> ht = new Hashtable<String, Integer>();
//	private TreeSet<String> ts = new TreeSet<String>();
	
	private IntWritable result = new IntWritable();
	public void reduce(Text key, Iterable<IntWritable> values,
            Context context
            ) throws IOException, InterruptedException {
		int count = 0;
		for(IntWritable i : values){
			 count += i.get();
		}
		
		 result.set(count);
//		 System.out.println(key.toString());
	     context.write(key, result);
		
	}

	
//	private String tagCounter(String tags){
//		/**
//		 *
//		 * input: {tag1 tag2 tag3 tag1 tag4 tag2 tag2}
//		 * output:{7(total number of tags) 2#tag1 3#tag2 1#tag3 1#tag4}
//		 */
//		if(tags.equals("")) return tags;
//		
//		ht.clear();
//		
//		String[] allTags = tags.split(" ");
//		
//	
//		
//		int totalNumberOfTags = allTags.length;
//		
//
//		for(int i = 0; i < totalNumberOfTags; i++){
//			String currTag = allTags[i];
//			if(ht.containsKey(currTag)){
//				ht.put(currTag, ht.get(currTag) + 1);
//			}else{
//				ht.put(currTag, 1);
//			}
//		}
//		
//		String formTags = Integer.toString(totalNumberOfTags);
//		
//		//sort tags???
//		//TreeSet<String> ts = new TreeSet(ht.keySet());
//	//	ts.clear();
//	//	ts.addAll(ht.keySet());
//		
//		//ts or ht.keySet
//		for(Entry<String,Integer> e : ht.entrySet()){
//			formTags = formTags + " "  + Integer.toString(e.getValue()) + "#" + e.getKey();
//		}
////		for(String tag : ts){
////		
////			formTags = formTags + " "  + Integer.toString(ht.get(tag)) + "#" + tag;
////		}
//
//
//		return formTags;
//	}
}