package mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job4Mapper1
			extends Mapper<Object, Text, Text, Text>{
			
			
			private Text place_id = new Text();
			private Text tags = new Text();
			public void map(Object key, Text value, Context context
			             ) throws IOException, InterruptedException {
			
				//System.out.println("Job4 mapper1");
			
				/**
				 * place-id total-photo-count country-name
				 */
				
				//get place-id and tags
				String[] split = value.toString().split("\t");
				
				place_id.set(split[0] + ":1");
				//String[] splitCity = split[1].split(" ");
				tags.set("++++" + split[1]);
				//System.out.println(split[0] + "->" + tags.toString());
				context.write(place_id, tags);

			}
}