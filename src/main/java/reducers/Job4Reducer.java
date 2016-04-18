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
		
		//	int totalTagCount = 0;
			recordSet.clear();
			for(Text t: values){
		
				
				String[] splitVal = t.toString().split("/");
				int currTagCount = Integer.parseInt((splitVal[1]));
		//		totalTagCount += currTagCount;
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
				resultValue += " " + "(" + currPair.value + ":" + currPair.key + ")";
				
			}
			
			context.write(new Text(city.replace("+", " ")), new Text(resultValue));

		}
		

}