package reducers;

import java.io.IOException;

import java.util.Hashtable;
import java.util.TreeSet;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1Reducer
		extends Reducer<Text,Text,Text,Text> {
	
	//	private Hashtable<String, Integer> ht = new Hashtable<String, Integer>();
	//	private TreeSet<String> ts = new TreeSet<String>();
		private Text city = new Text();
		private Text pc = new Text();
	
		public void reduce(Text key, Iterable<Text> values,
		                Context context
		                ) throws IOException, InterruptedException {

		int photoCount = 0;
		//String temp = null;
	
		for (Text val : values) {
			String value = val.toString();
				
			if(value.startsWith("----")){
				city.set(value.substring(4));
			}else if(value.startsWith("++++")){
				photoCount++;
			}

		}
	
	//	if(temp == null) temp = "";
		
		//temp = tagCounter(temp);

		
	//	temp = Integer.toString(photoCount) + "/" + temp;
		
		//if(photoCount != 0) System.out.println(temp);
		if(photoCount!= 0){
			pc.set(Integer.toString(photoCount) + "/" + key.toString());
			context.write(city, pc);
		
		}
	
		}
		
	
//		private String tagCounter(String tags){
//			/**
//			 *
//			 * input: {tag1 tag2 tag3 tag1 tag4 tag2 tag2}
//			 * output:{7(total number of tags) 2#tag1 3#tag2 1#tag3 1#tag4}
//			 */
//			if(tags.equals("")) return tags;
//			
//			ht.clear();
//			
//			String[] allTags = tags.split(" ");
//			
//		
//			
//			int totalNumberOfTags = allTags.length;
//			
//
//			for(int i = 0; i < totalNumberOfTags; i++){
//				String currTag = allTags[i];
//				if(ht.containsKey(currTag)){
//					ht.put(currTag, ht.get(currTag) + 1);
//				}else{
//					ht.put(currTag, 1);
//				}
//			}
//			
//			String formTags = Integer.toString(totalNumberOfTags);
//			
//			//sort tags???
//			//TreeSet<String> ts = new TreeSet(ht.keySet());
//			ts.clear();
//			ts.addAll(ht.keySet());
//			
//			//ts or ht.keySet
//			for(String tag : ts){
//				formTags = formTags + " "  + Integer.toString(ht.get(tag)) + "#" + tag;
//			}
//
//
//			return formTags;
//		}
}