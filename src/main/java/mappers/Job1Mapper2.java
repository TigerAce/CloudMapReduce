package mappers;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job1Mapper2
			extends Mapper<Object, Text, Text, Text>{
			
	
			private Text place_id = new Text();
			private Text locality = new Text();
			public void map(Object key, Text value, Context context
			             ) throws IOException, InterruptedException {
			
			
			
				/**
				 * place-id \t woeid \t latitude \t longitude \t place-name \t place-type-id \t place-url
				 */		
				
				
				//get place-id and tags
				String[] splitRecord = value.toString().split("\t");
				
				place_id.set(splitRecord[0]);
				
				//filter: only accept place-type-id = {7,22}
				if(splitRecord[5].equals("7")){
					//the record is locality level
					String placeURL = splitRecord[6];
					String[] splitURL = placeURL.split("/");
					String city = splitURL[splitURL.length - 1];
					
					locality.set("----" + city);
					context.write(place_id, locality);
					
				}else if(splitRecord[5].equals("22")){
					//the record is neighborhood level
					String placeURL = splitRecord[6];
					String[] splitURL = placeURL.split("/");
					String city = splitURL[splitURL.length - 2];
					
					locality.set("----" + city);
					context.write(place_id, locality);
					
				}
					

			}
}