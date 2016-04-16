package reducers;

import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.TreeSet;

import org.apache.commons.math3.util.MultidimensionalCounter.Iterator;
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
			
			int count = 0;
			
			HashSet<String> hs = new HashSet<String>();
		
			for(Text t: values){
				String[] splitVals = t.toString().split("/");
				
				int c = Integer.parseInt(splitVals[0]);
				count += c;
				
				String[] splitIDs = splitVals[1].split(" ");
				for(int i = 0; i < splitIDs.length; i++){
				//	if(!splitIDs[i].equals(""))
					hs.add(splitIDs[i]);
				}
				
				//placeIds += " " + splitVals[1];
			}

			String placeIds = "";
			for(java.util.Iterator<String> iter = hs.iterator(); iter.hasNext();){
				String n = iter.next();
				if(placeIds.equals(""))
				placeIds = n;
				else
				placeIds += " " + n;
			}
			context.write(key, new Text(Integer.toString(count) + "/" + placeIds));
		//	System.out.println("job1 red");
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