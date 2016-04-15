package reducers;

import java.io.IOException;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;




public class Job4Reducer
		extends Reducer<Text,Text,Text,Text> {
	
	private TreeSet<DataPair> recordSet = new TreeSet<DataPair>();
//		Hashtable<String, Integer> combinedTags = new Hashtable<String,Integer>();
//		int totalTagCount;
		
		
		private class DataPair implements Comparable<DataPair>{

			public int key;
			public String value;
			
			DataPair(int key, String value){
				this.key = key;
				this.value = value;
			}
			
			public int compareTo(DataPair dp) {
				if(this.key < dp.key)
					return -1;
				else if(this.key == dp.key){

					int c = this.value.compareTo(dp.value);
					if(c < 0){
						return 1;
					}else if(c > 0){
						return -1;
					}
//					else if(c == 0){
//						return 0;
//					}
					
				}else return 1;
					
				
				return 0;
			}

			
			@SuppressWarnings("unused")
			public boolean equals(DataPair dp) {
				return this.value.compareTo(dp.value) == 0;		
			}
			
			
		}
		
		
		public void reduce(Text key, Iterable<Text> values,
		                Context context
		                ) throws IOException, InterruptedException {
		//	System.out.println("-----reduce----");
			int totalTagCount = 0;
			recordSet.clear();
			for(Text t: values){
			//	System.out.println(key.toString() + "->" + t.toString());
				
				String[] splitVal = t.toString().split("/");
				int currTagCount = Integer.parseInt((splitVal[1]));
				totalTagCount += currTagCount;
				recordSet.add(new DataPair(currTagCount, splitVal[0]));
				
				//push out the first record if the map size greater than 50
				if(recordSet.size() > 10){
					recordSet.remove(recordSet.first());
				}
			}
			
			String[] splitCity = key.toString().split(":");
			String city = splitCity[0];
			String totalPhotoCount = splitCity[1];
			
			String resultValue = totalPhotoCount;
			for(Iterator<DataPair> iter = recordSet.descendingIterator(); iter.hasNext();){
				DataPair currPair = iter.next();
				resultValue += " " + "(" + currPair.value + ":" + (float)currPair.key/totalTagCount * 100 + "%)";
				
			}
			
			context.write(new Text(city), new Text(resultValue));
//			String k = key.toString();
//			String[] splitKey = k.split(":");
//			String country = splitKey[0].replace("+", " ");
//			String totalPhotoNumber = splitKey[1];
//			
//			combinedTags.clear();
//			totalTagCount = 0;
//			
//			
//			//System.out.println("---------reduce--------");
//			for(Text val : values){
//				//System.out.println(key.toString() + "->" + "tags");
//				tagAggregator(val.toString());
//			}
//			
//			String resTags = countTagFrequency(totalTagCount,combinedTags);
//			
//			context.write(new Text(country), new Text(totalPhotoNumber + " " + resTags));
		}
		
//		private void tagAggregator(String tagString){
//			/**
//			 * the format of tag String is :
//			 * {7(total number of tags) 2#tag1 3#tag2 1#tag3 1#tag4}
//			 * 
//			 * need to add to the hashtable
//			 */
//			
//			String[] tags1 = tagString.split(" ");
//		
//			
//			totalTagCount += Integer.parseInt(tags1[0]);
//			
//			for(int i = 1; i < tags1.length; i++){
//				String currTag = tags1[i];
//				String[] splitCurrTag = currTag.split("#");
//				
//				if(splitCurrTag.length == 2){
//					String t = splitCurrTag[1];
//					int c = Integer.parseInt(splitCurrTag[0]);
//					
//					if(combinedTags.containsKey(t)){
//						combinedTags.put(t, combinedTags.get(t) + c);
//					}else{
//						combinedTags.put(t, c);
//					}
//				}
//			}
//			
//		}
//		
//		private String countTagFrequency(int totalTagCount, Hashtable<String, Integer> tags){
//			
//			TreeSet<DataPair> ts = new TreeSet<DataPair>();
//
//			for(Entry<String,Integer> e : tags.entrySet()){
//				DataPair currPair = new DataPair(e.getValue(),e.getKey());
//				
//				ts.add(currPair);
//				
//				//only record top 10 tags
//				if(ts.size() > 10){
//					ts.remove(ts.first());
//				}
//			}
//				
//			String resultTags = null;
//			
//			for(Iterator<DataPair> iter = ts.descendingIterator(); iter.hasNext();){
//				DataPair currPair = iter.next();
//				currPair.value = "(" + currPair.value + ":" + (float)currPair.key/totalTagCount * 100 + "%)";
//				
//				if(resultTags != null){
//					resultTags += " " + currPair.value;
//				}else{
//					resultTags = currPair.value;
//				}
//			}
//			return resultTags;
//		}
}