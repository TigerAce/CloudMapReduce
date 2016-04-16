package mappers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job3Mapper1
			extends Mapper<Object, Text, Text, IntWritable>{
			
	
	private Hashtable<String, String> ht = new Hashtable<String,String>();
	private HashMap hm = new HashMap();	
			@Override
	protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	//	System.out.println("setup");
		try{
			Configuration conf = context.getConfiguration();
            Path pt=new Path(conf.get("job2res") + "/part-r-00000");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line=br.readLine();
            while (line != null){
                    String[] splitLine = line.split("\t");
          //          System.out.println(splitLine[0] + "->" + splitLine[1]);
                    ht.put(splitLine[0], splitLine[1]);
                    line=br.readLine();
            }
            }catch(Exception e){
            }
	//	System.out.println(ht.size());
	}
		
	

			public void map(Object key, Text value, Context context
			             ) throws IOException, InterruptedException {
				/**
				 * photo-id \t owner \t tags \t date-taken \t place-id \t accuracy
				 */

				//get place-id and tags
				String[] split = value.toString().split("\t");
				
		//		if(ht.containsKey("bQ4bn4qbAZz3HxQT4A"))System.out.println("ccc");
				String placeId = split[4];
			//	System.out.println(placeId);
			//	System.out.println(ht.size());
				if(ht.containsKey(placeId)){
		//			System.out.println("contains");
					String tags = split[2];
					String city = ht.get(placeId);
					//System.out.println(city);
					//System.out.println(ci);
					//System.out.println(country);
					String[] splitTags = tags.split(" ");
					for(int i = 0; i < splitTags.length; i++){
						String currTag = splitTags[i];
						if(!currTag.equals("") )
						{
							if(tagFilter(splitTags[i],city))
							{
								context.write(new Text(splitTags[i] + " " + city), new IntWritable(1));	
							}
				
						}
					}
				}
			
			}
			
			public static boolean tagFilter(String str,String city)
			{
				String split1[] = city.split(",");
				String ci = split1[0];
				String province = split1[1];
				String split2[] = split1[2].split("/");
				String country = split2[0];
				
				if(isNumeric(str))
				{
					int year = Integer.parseInt(str);
					if(year<=2016 && year>=1000)
					{
						return false;
					}else
					{
						return true;
					}
				}else 
				{
					if(str.equalsIgnoreCase(ci)||str.equalsIgnoreCase(country)
							||str.equalsIgnoreCase(country.replace("+",""))
							||str.equalsIgnoreCase(ci.replace("+",""))
							||str.equalsIgnoreCase(province)
							||str.equalsIgnoreCase(province.replace("+","")))
					{
						return false;
					}else
					{
						return true;
					}
				}
				
			}
			
			public static boolean isNumeric(String str)
			  {
			    try
			    {
			      int i = Integer.parseInt(str);
			    }
			    catch(NumberFormatException nfe)
			    {
			      return false;
			    }
			    return true;
			  }
}