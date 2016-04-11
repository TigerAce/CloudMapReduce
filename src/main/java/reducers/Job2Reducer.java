package reducers;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Job2Reducer
		extends Reducer<Text,Text,Text,Text> {
	
	private Text value = new Text();
	//ArrayList<String> placeIDRecord = new ArrayList<String>();
	public void reduce(Text key, Iterable<Text> values,
            Context context
            ) throws IOException, InterruptedException {
	//	System.out.println("key:" + key.toString());
	//	System.out.println("values");
		
	//	placeIDRecord.clear();
		int totalNumberOfPhotos = 0;
		
		String placeIds = "";
		
		for(Text t : values){
			String currVal = t.toString();
			String[] splitVal = currVal.split("/");
			
			int currNumberOfPhotos = Integer.parseInt(splitVal[0]);
			String placeId = splitVal[1];
			placeIds += " " + placeId;
		//	placeIDRecord.add(placeId);
			//if there is photos
			if(currNumberOfPhotos > 0){
				//add photo number to total
				totalNumberOfPhotos += currNumberOfPhotos;
			}
		}
		
		
	//	for(String id : placeIDRecord){
			context.write(key, new Text(Integer.toString(totalNumberOfPhotos) + "/" + placeIds));
	//	}
//		value.set(Integer.toString(totalNumberOfPhotos) + "/" + Integer.toString(totalNumberOfTags) + " " + totalTags);
//		context.write(key, value);
		
	}
	
	
}
	