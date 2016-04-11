package mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job1Mapper1
			extends Mapper<Object, Text, Text, Text>{
			
			private final static IntWritable one = new IntWritable(1);
			private Text place_id = new Text();
			private Text count = new Text();
			public void map(Object key, Text value, Context context
			             ) throws IOException, InterruptedException {
			
				
			
				/**
				 * photo-id \t owner \t tags \t date-taken \t place-id \t accuracy
				 */
				
				//get place-id and tags
				String[] split = value.toString().split("\t");
				
				place_id.set(split[4]);
				count.set("++++");
				context.write(place_id, count);

			}
}