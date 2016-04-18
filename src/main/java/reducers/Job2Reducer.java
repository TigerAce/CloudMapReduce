package reducers;

import java.io.IOException;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;




public class Job2Reducer
		extends Reducer<Text,Text,Text,Text> {
	
	private TreeSet<DataPair> recordSet = new TreeSet<DataPair>();

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
	
		/**
		 * CAN NOT USE TREE MAP TO SORT THE RESULTS AS IT DOES NOT ACCEPT DUPLICATE KEY
		 */
		/**
		 * key = "k"   values = set of value where 
		 * value = locality \t total-photo-number/total-tag-number num1#tag1 num2#tag2 num3#tag3 ... num n#tag n
		 *
		 *
		 * TODO: 
		 * 1.split out total photo number
		 * 2.put total photo number in key of tree map and rest info in value
		 * 3.sort tree map in descending order
		 * 4.for top 50 map record: do process tag info by aggregate the same tags and count the frequency
		 * 5.out put the 50 record
		 */
		
		//as all data has the same key, so the reduce function will only be called once, maybe there is no need to clear the set.
		recordSet.clear();
		
		
		for(Text value: values){
			
			String val = value.toString();
		//	System.out.println(val);
			String locality = null;
			int totalNumberOfPhotos = 0;
			
			
			String[] splitLocality = val.split("\t");
			locality = splitLocality[0];
			
			String[] splitNumPhotos = splitLocality[1].split("/");
			totalNumberOfPhotos = Integer.parseInt(splitNumPhotos[0]);
			
			//if photo number != 0 -> process tags
			if(totalNumberOfPhotos != 0){
					String PlaceIdInfo = splitNumPhotos[1];
					//handle possible extra split
					if(splitNumPhotos.length > 2){		
						for(int i = 2; i < splitNumPhotos.length; i++){
							PlaceIdInfo += splitNumPhotos[i];
						}
					}
					
					/**
					 * form a key pair where (key = totalNumberOfPhotos, value = locality + tag info)
					 * and insert into tree set for sorting process
					 */
					//form value string
					String allInfo = locality + "\t" + totalNumberOfPhotos + "\t" + PlaceIdInfo;
					recordSet.add(new DataPair(totalNumberOfPhotos, allInfo));
					
					//push out the first record if the map size greater than 50
					if(recordSet.size() > 50){
						recordSet.remove(recordSet.first());
					}
					
			}
			
		
		}
		
		
		/**
		 * now we have top 50 record stored in tree map
		 * 
		 */

	
		int i = 0;
		for(Iterator<DataPair> iter = recordSet.descendingIterator(); iter.hasNext();){
		
			
			DataPair currPair = iter.next();
			i++;
			String data = currPair.value;
			String[] splitData = data.split("\t");
			
			String locality = splitData[0];
			String photoCount = splitData[1];
			String placeIdInfo = splitData[2];
			

			String[] splitPlaceId = placeIdInfo.split(" ");
			for(String s : splitPlaceId){
				if(!s.equals(""))
				context.write(new Text(s), new Text(locality + "/" + Integer.toString(i) + "/" +  photoCount));
				
			}

					
		}

		
	}

	
}