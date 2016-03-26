package drivers;

import java.util.*;


import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.*;

import mappers.*;
import reducers.*;



public class TopPlaceByNumOfPhotos extends Configured implements Tool{

	private static final String INTERMEDIATE_OUTPUT1 = "/Users/chen/Desktop/intermediate1";
	private static final String INTERMEDIATE_OUTPUT2 = "/Users/chen/Desktop/intermediate2";
	
	public static void main(String[] args) throws Exception {
			     int res = ToolRunner.run(new Configuration(), new TopPlaceByNumOfPhotos(), args);
			     System.exit(res);
	}

	public int run(String[] args) throws Exception {




//		 	Configuration conf = new Configuration();
//
//		 	Job job = Job.getInstance(conf, "Show top 50 locality-level places based on the number of photos taken in each locality");
//		    job.setJarByClass(TopPlaceByNumOfPhotos.class);
//
//		    //set mapper
//		    job.setMapperClass(RecordMapper.class);
//
//		    //set combiner
//		    job.setCombinerClass(RecordReducer.class);
//
//		    //set reducer
//		    job.setReducerClass(RecordReducer.class);
//
//		    //set output format
//		    job.setOutputKeyClass(Text.class);
//		    job.setOutputValueClass(Text.class);
//
//		    //set input and output path
//		    FileInputFormat.addInputPath((JobConf)job.getConfiguration(), new Path(args[0]));
//		    FileOutputFormat.setOutputPath((JobConf)job.getConfiguration(), new Path(args[1]));
//
//		    return job.waitForCompletion(true) ? 0 : 1;




		/**
		 * Job1 driver
		 */

		Configuration conf1 = new Configuration();

	 	Job job1 = Job.getInstance(conf1, "Show top 50 locality-level places based on the number of photos taken in each locality");
	    job1.setJarByClass(TopPlaceByNumOfPhotos.class);

	    //set mapper
	    job1.setMapperClass(Job1Mapper1.class);

	    //set combiner
	  //  job.setCombinerClass(RecordReducer.class);

	    //set reducer
	    
	    /**
	     * set reducer number    a partitioner?
	     */
	    job1.setNumReduceTasks(3);
	    job1.setReducerClass(Job1Reducer.class);

	    //set output format
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);

	    //set input and output path
	  //  FileInputFormat.addInputPath((JobConf)job.getConfiguration(), new Path(args[0]));
	   MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, Job1Mapper1.class);
	   MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, Job1Mapper2.class);

	   FileOutputFormat.setOutputPath(job1, new Path(this.INTERMEDIATE_OUTPUT1));

	 
	   
	   /**
	    * job 2 driver
	    */
		Configuration conf2 = new Configuration();

	 	Job job2 = Job.getInstance(conf2, "combine results with same locality level place");
	    job2.setJarByClass(TopPlaceByNumOfPhotos.class);

	    //set mapper
	    
	    job2.setMapperClass(Job2Mapper1.class);

	    //set combiner
	  //  job.setCombinerClass(RecordReducer.class);

	    //set reducer
	    job2.setReducerClass(Job2Reducer.class);

	    //set output format
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);

	    //set input and output path
	    FileInputFormat.addInputPath(job2, new Path(this.INTERMEDIATE_OUTPUT1 + "/part*"));
	   
	    FileOutputFormat.setOutputPath(job2, new Path(this.INTERMEDIATE_OUTPUT2));
	    
	    
	    
	    /**
	     * job 3 dirver
	     */
	    
		Configuration conf3 = new Configuration();

	 	Job job3 = Job.getInstance(conf3, "sort by number of photos");
	    job3.setJarByClass(TopPlaceByNumOfPhotos.class);

	    //set mapper
	    
	    job3.setMapperClass(Job3Mapper1.class);

	    //set combiner
	  //  job.setCombinerClass(RecordReducer.class);

	    //set reducer
	    job3.setReducerClass(Job3Reducer.class);

	    //set output format
	    job3.setOutputKeyClass(Text.class);
	    job3.setOutputValueClass(Text.class);

	    //set input and output path
	    FileInputFormat.addInputPath(job3, new Path(this.INTERMEDIATE_OUTPUT2 + "/part*"));
	   
	    FileOutputFormat.setOutputPath(job3, new Path(args[2]));
	
	    
	    
	    /**
	     * job control
	     */

	    
	    job1.waitForCompletion(true);
	    job2.waitForCompletion(true);
	    return job3.waitForCompletion(true)? 0 : 1;
	    
	    
//		JobControl jc = new JobControl("First Taks Group");
//		
//	    ControlledJob cjob1 = new ControlledJob(job1,null);
//		ControlledJob cjob2 = new ControlledJob(job2,null);
//		
//		cjob2.addDependingJob(cjob1);
//		jc.addJob(cjob1);
//		jc.addJob(cjob2);
//		
//		jc.run();
//		
//		return 1;
//		   
		   
		   
	   // return job1.waitForCompletion(true) ? 0 : 1;


	}

}
