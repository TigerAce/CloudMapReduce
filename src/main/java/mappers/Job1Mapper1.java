package mappers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job1Mapper1
			extends Mapper<Object, Text, Text, Text>{
			
			private final static IntWritable one = new IntWritable(1);
			private Text place_id = new Text();
			private Text count = new Text();
			private Hashtable<String, String> ht = new Hashtable<String,String>();

			/**
			 * 
			 * 
			 * TODO: load place txt
			 */
			protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				super.setup(context);
						try{
					Configuration conf = context.getConfiguration();
					
		            Path pt=new Path(conf.get("place"));
		            FileSystem fs = FileSystem.get(new Configuration());
		            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		            String line;
		            line=br.readLine();
		            while (line != null){
		            	//place-id \t woeid \t latitude \t longitude \t place-name \t place-type-id \t place-url
		            	
		            	/**
		            	 * TODO: note that some neighbor's name under different city may be identical.
		            	 */
		                    String[] splitLine = line.split("\t");
		                    if(splitLine[5].equals("7"))
							{
								//the record is locality level
								String placeURL = splitLine[6];
								String[] splitURL = placeURL.split("/");
								String city = splitURL[splitURL.length - 1];
								ht.put(splitLine[0], city);
							//	city_info.put(splitRecord[0],city);

								
							}else if(splitLine[5].equals("22"))
							{
								//the record is neighborhood level
								String placeURL = splitLine[6];
								String[] splitURL = placeURL.split("/");
								String city = splitURL[splitURL.length - 2];
								ht.put(splitLine[0], city);
							}

		                   // ht.put(splitLine[0], splitLine[6]);
		                    line=br.readLine();
		            }
		            }catch(Exception e){
		            }
			
			}
		
			
			public void map(Object key, Text value, Context context
			             ) throws IOException, InterruptedException {
			
				
			
				/**
				 * photo-id \t owner \t tags \t date-taken \t place-id \t accuracy
				 */
				
				//get place-id and tags
				String[] split = value.toString().split("\t");
				
				String placeId = split[4];
				
				if(ht.containsKey(placeId)){
					String city = ht.get(placeId);
					context.write(new Text(city), new Text("1/" + placeId));
				}
//				place_id.set(split[4]);
//				count.set("++++");
//				context.write(place_id, count);

			}
}