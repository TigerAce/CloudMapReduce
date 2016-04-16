package mappers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job2Mapper1
			extends Mapper<Object, Text, Text, Text>{
			
	
			private Text k = new Text();

			public void map(Object key, Text value, Context context) 
					throws IOException, InterruptedException {		
//System.out.println("job3 mapper");
				k.set("k");
				context.write(k, value);
			}
}