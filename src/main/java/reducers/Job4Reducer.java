package reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Job4Reducer
		extends Reducer<Text,Text,Text,Text> {
	
	private Hashtable<String, Integer> ht = new Hashtable<String, Integer>();
	private TreeSet<String> ts = new TreeSet<String>();
	
	public void reduce(Text key, Iterable<Text> values,
            Context context
            ) throws IOException, InterruptedException {
		
		
		String k = key.toString();
		String[] splitKey = k.split(":");
		if(splitKey[1].equals("1")){
		
			String city = null;
			String totalPhotoNumber = null;
			
			String t = null;
			for(Text val : values){
				//System.out.println(key.toString() + "->" + "val");
				String value = val.toString();
				
				if(value.startsWith("----")){
					//from photo
					String tags = value.substring(4);
					if(t == null){
						t = tags;
					}else{
						t += " " + tags;
					}
					
					
				}else if(value.startsWith("++++")){
					//from top 50 city
					String[] splitVal = value.substring(4).split(" ");
					totalPhotoNumber = splitVal[0];
					city = splitVal[1];
				}
			}
			
			if(t != null && city != null && totalPhotoNumber != null){
				t = tagCounter(t);
				context.write(new Text(city), new Text(totalPhotoNumber + "/" + t));
			}
		
		}
		
		
	}
	
	private String tagCounter(String tags){
		/**
		 *
		 * input: {tag1 tag2 tag3 tag1 tag4 tag2 tag2}
		 * output:{7(total number of tags) 2#tag1 3#tag2 1#tag3 1#tag4}
		 */
		if(tags.equals("")) return tags;
		
		ht.clear();
		
		String[] allTags = tags.split(" ");
		
	
		
		int totalNumberOfTags = allTags.length;
		

		for(int i = 0; i < totalNumberOfTags; i++){
			String currTag = allTags[i];
			if(ht.containsKey(currTag)){
				ht.put(currTag, ht.get(currTag) + 1);
			}else{
				ht.put(currTag, 1);
			}
		}
		
		String formTags = Integer.toString(totalNumberOfTags);
		
		//sort tags???
		//TreeSet<String> ts = new TreeSet(ht.keySet());
		ts.clear();
		ts.addAll(ht.keySet());
		
		//ts or ht.keySet
		for(String tag : ts){
			formTags = formTags + " "  + Integer.toString(ht.get(tag)) + "#" + tag;
		}


		return formTags;
	}
}