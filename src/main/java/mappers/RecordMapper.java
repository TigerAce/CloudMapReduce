package mappers;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RecordMapper
			extends Mapper<Object, Text, Text, Text>{
			
			private final static IntWritable one = new IntWritable(1);
			private Text place_id = new Text();
			private Text tags = new Text();
			public void map(Object key, Text value, Context context
			             ) throws IOException, InterruptedException {
			
				System.out.println(value.toString());
			
				/**
				 * photo-id \t owner \t tags \t date-taken \t place-id \t accuracy
				 */
				
				//get place-id and tags
				String[] split = value.toString().split("\t");
				
				place_id.set(split[4]);
				tags.set(split[2]);
				context.write(place_id, tags);
				//context.write(new Text(split[4]), new Text(split[2]));

				
//					System.out.println(value.toString());
//					StringTokenizer itr = new StringTokenizer(value.toString());
//					
//					while (itr.hasMoreTokens()) {
//							 word.set(itr.nextToken());
//							 context.write(word, one);
//					}
			}
}