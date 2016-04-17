package drivers;


import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.*;

import mappers.*;
import reducers.*;



public class TaskDriver extends Configured implements Tool{

	private static final String INTERMEDIATE_OUTPUT1 = "./intermediate1";
	private static final String INTERMEDIATE_OUTPUT2 = "./intermediate2";
	private static final String INTERMEDIATE_OUTPUT3 = "./intermediate3";
	private static final String INTERMEDIATE_OUTPUT4 = "./intermediate4";
	
	private static final int NUMBER_OF_NODES = 14;
	private static final int REDUCE_TASKS_MAXIMUM = 2;
	
	
	public static void main(String[] args) throws Exception {
			     int res = ToolRunner.run(new Configuration(), new TaskDriver(), args);
			    
			     System.exit(res);
	}

	public int run(String[] args) throws Exception {

		System.out.println(args[0] + " -> " + args[1] + " -> " + args[2]);

		/**
		 * Job1 driver
		 */

		Configuration conf1 = new Configuration();

		conf1.set("place", args[1]);
	 	Job job1 = Job.getInstance(conf1, "JOB1");
	    job1.setJarByClass(TaskDriver.class);

	    //set mapper
	    job1.setMapperClass(Job1Mapper1.class);

	    //set combiner
	    job1.setCombinerClass(Job1Reducer.class);

	    //set reducer
	    
	
	    job1.setNumReduceTasks((int) (1.75 * NUMBER_OF_NODES * REDUCE_TASKS_MAXIMUM));
	    job1.setReducerClass(Job1Reducer.class);

	    //set output format
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);

	    
	    
	   FileSystem fs; 

	   //get the FileStatus list from given dir
//	  FileStatus[] status_list = fs.listStatus(new Path(args[0]));
//	  if(status_list != null){
//	      for(FileStatus status : status_list){
//	          //add each file to the list of inputs for the map-reduce job
//	    	//  System.out.println(status.getPath());
//	         // FileInputFormat.addInputPath(conf, status.getPath());
//	    	  MultipleInputs.addInputPath(job1, new Path(status.getPath().toString()), TextInputFormat.class, Job1Mapper1.class);
//	      }
//	  }
	  
	   FileInputFormat.addInputPath(job1, new Path(args[0] + "/*"));
	  
	    //set input and output path
	  //  FileInputFormat.addInputPath((JobConf)job.getConfiguration(), new Path(args[0]));
	//   MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, Job1Mapper1.class);
	//   MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, Job1Mapper2.class);

	   /**
	     * clear out put path
	     */
	  
	  	fs = FileSystem.get(conf1);
	    /*Check if output path (args[1])exist or not*/
	    if(fs.exists(new Path(this.INTERMEDIATE_OUTPUT1))){
	       /*If exist delete the output path*/
	       fs.delete(new Path(this.INTERMEDIATE_OUTPUT1),true);
	    }
	    
	   FileOutputFormat.setOutputPath(job1, new Path(this.INTERMEDIATE_OUTPUT1));

	 
	   
//	   /**
//	    * job 2 driver
//	    */
//		Configuration conf2 = new Configuration();
//
//	 	Job job2 = Job.getInstance(conf2, "JOB2");
//	    job2.setJarByClass(TaskDriver.class);
//
//	    //set mapper
//	    
//	    job2.setMapperClass(Job2Mapper1.class);
//
//	    //set combiner
//	  //  job.setCombinerClass(RecordReducer.class);
//
//	    //set reducer
//	    job2.setReducerClass(Job2Reducer.class);
//
//	    //set output format
//	    job2.setOutputKeyClass(Text.class);
//	    job2.setOutputValueClass(Text.class);
//
//	    //set input and output path
//	    FileInputFormat.addInputPath(job2, new Path(this.INTERMEDIATE_OUTPUT1 + "/part*"));
//	   
//	    /**
//	     * clear out put path
//	     */
//	    fs = FileSystem.get(conf2);
//	    /*Check if output path (args[1])exist or not*/
//	    if(fs.exists(new Path(this.INTERMEDIATE_OUTPUT2))){
//	       /*If exist delete the output path*/
//	       fs.delete(new Path(this.INTERMEDIATE_OUTPUT2),true);
//	    }
//	    FileOutputFormat.setOutputPath(job2, new Path(this.INTERMEDIATE_OUTPUT2));
//	    
	    
	    
	    /**
	     * job 2 dirver
	     */
	    
		Configuration conf2 = new Configuration();

	 	Job job2 = Job.getInstance(conf2, "JOB2");

	    job2.setJarByClass(TaskDriver.class);

	    //set mapper
	    
	    job2.setMapperClass(Job2Mapper1.class);

	    //set combiner
	  //  job.setCombinerClass(RecordReducer.class);
	//    job3.setNumReduceTasks((int) (1.75 * NUMBER_OF_NODES * REDUCE_TASKS_MAXIMUM));
	    //set reducer
	    job2.setReducerClass(Job2Reducer.class);

	    //set output format
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);

	    //set input and output path
	    FileInputFormat.addInputPath(job2, new Path(this.INTERMEDIATE_OUTPUT1 + "/part*"));
	   
	    /**
	     * clear out put path
	     */
	    fs = FileSystem.get(conf2);
	    /*Check if output path (args[1])exist or not*/
	    if(fs.exists(new Path(this.INTERMEDIATE_OUTPUT2))){
	       /*If exist delete the output path*/
	       fs.delete(new Path(this.INTERMEDIATE_OUTPUT2),true);
	    }
	    FileOutputFormat.setOutputPath(job2, new Path(this.INTERMEDIATE_OUTPUT2));
	
	    
	    
	    /**
	     * job 3 dirver
	     */
	    
		Configuration conf3 = new Configuration();

		conf3.set("job2res", this.INTERMEDIATE_OUTPUT2);

	 	Job job3 = Job.getInstance(conf3, "JOB3");

	    job3.setJarByClass(TaskDriver.class);

	    //set mapper
	    
	    job3.setMapperClass(Job3Mapper1.class);

	    //set combiner
	   job3.setCombinerClass(Job3Reducer.class);
	  //  job4.setNumReduceTasks(2);
//	    if(fs.exists(p)){ 
//	    	fs.delete(p, true); 

//	    	}
	    
	   job3.setNumReduceTasks((int) (1.75 * NUMBER_OF_NODES * REDUCE_TASKS_MAXIMUM));
	    
//	    job4.setPartitionerClass(NaturalKeyPartitioner.class);
//	    job4.setGroupingComparatorClass(GroupComprator.class);
//	    job4.setSortComparatorClass(KeyComprator.class);
	    
	   
	    //set reducer
	    
	    job3.setReducerClass(Job3Reducer.class);
	    
	    
	    //set output format
	    job3.setOutputKeyClass(Text.class);
	    job3.setOutputValueClass(IntWritable.class);
	    
	
//		  if(status_list != null){
//		      for(FileStatus status : status_list){
//		          
//		    	  MultipleInputs.addInputPath(job4, new Path(status.getPath().toString()), TextInputFormat.class, Job4Mapper1.class);
//		      }
//		  }
//		  
		  
//		  FileSystem fs4= FileSystem.get(conf4); 
//		   //get the FileStatus list from given dir
//		  FileStatus[] status_list4 = fs4.listStatus(new Path(this.INTERMEDIATE_OUTPUT3));
//		  if(status_list4 != null){
//		      for(FileStatus status : status_list4){
//		    	  MultipleInputs.addInputPath(job4, new Path(status.getPath().toString()), TextInputFormat.class, Job4Mapper1.class);
//		      }
//		  }
		  
		    //set input and output path
		  //  FileInputFormat.addInputPath((JobConf)job.getConfiguration(), new Path(args[0]));
	//	   MultipleInputs.addInputPath(job4, new Path(this.INTERMEDIATE_OUTPUT3 + "/part-r-00000"), TextInputFormat.class, Job4Mapper1.class);
		 //  MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, Job1Mapper2.class);
	    //set input and output path
	
		  FileInputFormat.addInputPath(job3, new Path(args[0] + "*"));
	   
		  
		  /**
		     * clear out put path
		     */
		    fs = FileSystem.get(conf3);
		    /*Check if output path (args[1])exist or not*/
		    if(fs.exists(new Path(this.INTERMEDIATE_OUTPUT3))){
		       /*If exist delete the output path*/
		       fs.delete(new Path(this.INTERMEDIATE_OUTPUT3),true);
		    }
	    FileOutputFormat.setOutputPath(job3, new Path(this.INTERMEDIATE_OUTPUT3));
	    
	    
	    
	    
	    
	    
	    /**
	     * job 4 dirver
	     */
	    
		Configuration conf4 = new Configuration();

	 	Job job4 = Job.getInstance(conf4, "JOB4");

	    job4.setJarByClass(TaskDriver.class);

	    //set mapper
	    
	   
	    job4.setMapperClass(Job4Mapper1.class);

	    //set combiner
	  //  job.setCombinerClass(RecordReducer.class);

	   // job5.setNumReduceTasks(50);
	    
	    //set reducer
	    job4.setReducerClass(Job4Reducer.class);

	    job4.setPartitionerClass(NaturalKeyPartitioner.class);
	    job4.setGroupingComparatorClass(GroupComprator.class);
	    job4.setSortComparatorClass(SortByPhotoNumber.class);
	    
	    //set output format
	    job4.setOutputKeyClass(Text.class);
	    job4.setOutputValueClass(Text.class);

	    //set input and output path
	    FileInputFormat.addInputPath(job4, new Path(this.INTERMEDIATE_OUTPUT3 + "/part*"));
	   
	    /**
	     * clear out put path
	     */
	    fs = FileSystem.get(conf4);
	    /*Check if output path (args[1])exist or not*/
	    if(fs.exists(new Path(args[2]))){
	       /*If exist delete the output path*/
	       fs.delete(new Path(args[2]),true);
	    }
	    FileOutputFormat.setOutputPath(job4, new Path(args[2]));
	    
	    
	    
	    /**
	     * job control
	     */

	    
	    
	    job1.waitForCompletion(true);
	    job2.waitForCompletion(true);
	    job3.waitForCompletion(true);
	    return job4.waitForCompletion(true)? 0 : 1;
	    


	}
	
	
	/**
	 * this comparator is used for Job 4 to make sure the result from job4mapper1 always show before the result from job4mapper2
	 * hence to decide whether to process the tags in the reducer.
	 */
	public static class KeyComprator extends WritableComparator {
		 
		 protected KeyComprator() {
		 super(Text.class, true);
		 }
		 
		 @Override
		 public int compare(WritableComparable w1, WritableComparable w2) {
		 
	
		 Text t1 = (Text) w1;
		 Text t2 = (Text) w2;
		 String[] t1Items = t1.toString().split(":");
		 String[] t2Items = t2.toString().split(":");
	
		 int comp = t1Items[0].compareTo(t2Items[0]);
		 
		//descending value
		 if (comp == 0) {
		 comp = -1 * t2Items[1].compareTo(t1Items[1]);
		 }
		 
		 return comp;
		 
		 }
		 }
	
	/**
	 * this comparator is used for Job 5 to sort the city by its total number of photos
	 */
	public static class SortByPhotoNumber extends WritableComparator {
		 
		 protected SortByPhotoNumber() {
		 super(Text.class, true);
		 }
		 
		 @Override
		 public int compare(WritableComparable w1, WritableComparable w2) {
		 
		//ascending zone and day
		 
		Text t1 = (Text) w1;
		 Text t2 = (Text) w2;
		 String[] t1Items = t1.toString().split(":");
		 String[] t2Items = t2.toString().split(":");

		 int i1 = Integer.parseInt(t1Items[1]);
		 int i2 = Integer.parseInt(t2Items[1]);
		 
		 if(i1 < i2){
			 return 1;
		 }else if(i1 == i2){
			 return 0;
		 }else{
			 return -1;
		 }
		 
		 }
		 }
	public static class GroupComprator extends WritableComparator {
		 
		 protected GroupComprator() {
		 super(Text.class, true);
		 }
		 
		 @Override
		 public int compare(WritableComparable w1, WritableComparable w2) {
		 
		
		 Text t1 = (Text) w1;
		 Text t2 = (Text) w2;
		 String[] t1Items = t1.toString().split(":");
		 String[] t2Items = t2.toString().split(":");
		 String t1Base = t1Items[0];
		 String t2Base = t2Items[0];
		 int comp = t1Base.compareTo(t2Base);
		 
		 return comp;
		 
		 }
		 }
	
	public class NaturalKeyPartitioner extends Partitioner<Text, Text> {
		 
	    @Override
	    public int getPartition(Text key, Text val, int numPartitions) {
	    	String[] keyItems = key.toString().split(":");
	    	String keyBase = keyItems[0];
	        int hash = keyBase.hashCode();
	        int partition = hash % numPartitions;
	        return partition;
	    }
	 
	}

	

}
