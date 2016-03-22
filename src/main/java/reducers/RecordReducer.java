package reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RecordReducer
		extends Reducer<Text,Text,Text,Text> {
		//private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<Text> values,
		                Context context
		                ) throws IOException, InterruptedException {
		
		System.out.println("in recuder");	
			
		//System.out.println("key = " + key.toString());
		for (Text val : values) {
		//	System.out.println(val.toString());
			context.write(key, val);
		}
		//result.set(sum);
		//context.write(key, result);
		}
}