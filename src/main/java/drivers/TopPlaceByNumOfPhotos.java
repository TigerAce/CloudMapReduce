package drivers;

import java.util.*;


import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.*;

import mappers.RecordMapper;
import reducers.RecordReducer;



public class TopPlaceByNumOfPhotos extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
			     int res = ToolRunner.run(new Configuration(), new TopPlaceByNumOfPhotos(), args);
			     System.exit(res);
	}
	
	public int run(String[] args) throws Exception {
		
		
		
		
		 	Configuration conf = new Configuration();
		   
		 	Job job = Job.getInstance(conf, "Show top 50 locality-level places based on the number of photos taken in each locality");
		    job.setJarByClass(TopPlaceByNumOfPhotos.class);
		    
		    //set mapper
		    job.setMapperClass(RecordMapper.class);
		    
		    //set combiner
		    job.setCombinerClass(RecordReducer.class);
		   
		    //set reducer
		    job.setReducerClass(RecordReducer.class);
		    
		    //set output format
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		   
		    //set input and output path
		    FileInputFormat.addInputPath((JobConf)job.getConfiguration(), new Path(args[0]));
		    FileOutputFormat.setOutputPath((JobConf)job.getConfiguration(), new Path(args[1]));
		   
		    return job.waitForCompletion(true) ? 0 : 1;
		
	}

}
