package reducers;

import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Job2Reducer
		extends Reducer<Text,Text,Text,Text> {
	
	private Text value = new Text();
	
	public void reduce(Text key, Iterable<Text> values,
            Context context
            ) throws IOException, InterruptedException {
	//	System.out.println("key:" + key.toString());
	//	System.out.println("values");
		
		int totalNumberOfPhotos = 0;
		int totalNumberOfTags = 0;
		String totalTags = null;
		
		
		for(Text t : values){
			String currVal = t.toString();
			String[] splitVal = currVal.split("/");
			
			int currNumberOfPhotos = Integer.parseInt(splitVal[0]);
			
			//if there is photos
			if(currNumberOfPhotos > 0){
				//add photo number to total
				totalNumberOfPhotos += currNumberOfPhotos;
				
				//Aggregate tag infos
				String tagInfo = splitVal[1];
				String[] splitTagInfo = tagInfo.split(" ");
				int currNumberOfTags = Integer.parseInt(splitTagInfo[0]);
				totalNumberOfTags += currNumberOfTags;
				
				//reform tags
				
				for(int i = 1; i < splitTagInfo.length; i++){
					if(totalTags == null) 
						totalTags = splitTagInfo[i];
					else 
						totalTags += " " + splitTagInfo[i];		
				}
			}
		}
		
		value.set(Integer.toString(totalNumberOfPhotos) + "/" + Integer.toString(totalNumberOfTags) + " " + totalTags);
		context.write(key, value);
		
	}
	
	
}
	