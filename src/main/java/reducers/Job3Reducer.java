package reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Job3Reducer
		extends Reducer<Text,IntWritable,Text,IntWritable> {

	
	private IntWritable result = new IntWritable();
	public void reduce(Text key, Iterable<IntWritable> values,
            Context context
            ) throws IOException, InterruptedException {
		int count = 0;
		for(IntWritable i : values){
			 count += i.get();
		}
		
		 result.set(count);

	     context.write(key, result);
		
	}


}