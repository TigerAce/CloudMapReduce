package mappers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job4Mapper1
			extends Mapper<Object, Text, Text, Text>{
			
	
			private Text k = new Text();

			public void map(Object key, Text value, Context context) 
					throws IOException, InterruptedException {		

				String[] split = value.toString().split("\t");
				String[] splitKey = split[0].split(" ");
				String[] ks = splitKey[1].split("/");
				String firstKey = ks[0];
				String secondKey = ks[2];
				
				String tag = splitKey[0] + "/" + split[1];
				
//				System.out.println(firstKey + ", " + secondKey + "->" + tag);
//				String[] splitPhtotCountAndTag = split[1].split("/");
//				k.set(split[0] + ":" + splitPhtotCountAndTag[0]);
				
				context.write(new Text(firstKey + ":" + secondKey), new Text(tag));
			}
}