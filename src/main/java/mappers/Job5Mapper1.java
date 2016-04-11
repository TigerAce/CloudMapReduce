package mappers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job5Mapper1
			extends Mapper<Object, Text, Text, Text>{
			
	
			private Text k = new Text();

			public void map(Object key, Text value, Context context) 
					throws IOException, InterruptedException {		

				String[] split = value.toString().split("\t");
				String[] splitPhtotCountAndTag = split[1].split("/");
				k.set(split[0] + ":" + splitPhtotCountAndTag[0]);
				
				context.write(k, new Text(splitPhtotCountAndTag[1]));
			}
}