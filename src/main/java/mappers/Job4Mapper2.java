package mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job4Mapper2
			extends Mapper<Object, Text, Text, Text>{
			
			
			private Text place_id = new Text();
			private Text tags = new Text();
			public void map(Object key, Text value, Context context
			             ) throws IOException, InterruptedException {
			
				//System.out.println("Job4 mapper2");
			
				/**
				 * photo-id \t owner \t tags \t date-taken \t place-id \t accuracy
				 */
				
				//get place-id and tags
				String[] split = value.toString().split("\t");
				
				place_id.set(split[4] + ":2");
				tags.set("----" + split[2]);
				context.write(place_id, tags);

			}
}