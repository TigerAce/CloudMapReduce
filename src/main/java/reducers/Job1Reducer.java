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
	
					hs.add(splitIDs[i]);
				}
				
	
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
	
		}

}