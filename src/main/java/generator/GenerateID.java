package generator;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * Raul Barth
 * Memory-efficient ID type and ID generator (MapReduce approach).
 * Input: 
 * 		<in>: input file
 * 		<ot>: output file where the ids batch will be stored for further audit
 * 		<iterations>: number of IDs to be generated in the batch
 * Output: 
 * 		- Ids are printed for analysis purpose
 * 		- The batch is stored in partitions inside the <out> folder
 */

public class GenerateID 
{
	private static int iterations;
	public static class Map  extends Mapper<Object, Text, Text, Text>{
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	System.out.println("----------IDS----------");
	    	for(int i=0;i<iterations;i++){
				final long counter = new AtomicLong(System.nanoTime() * 1000).getAndIncrement();
				System.out.println(counter);
				context.write(new Text("ID"), new Text(Long.toString(counter)));
			}
	    	
	    }
	}
	      	      
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		 if (args.length != 3) {
			  System.err.println("Usage: IDgenerator <in> <out> <iterations>");
			  System.exit(2);
		 }
		 iterations = Integer.parseInt(args[2]);		 		 
		 Configuration conf = new Configuration();
		 Job job = new Job(conf, "NZchallenge");
		 job.setJarByClass(GenerateID.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		 job.setMapperClass(Map.class);
		 job.setNumReduceTasks(1);
		 job.setInputFormatClass(TextInputFormat.class);
		 job.setOutputFormatClass(TextOutputFormat.class);
		 
		 DateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy-hhmmss");
		 job.getConfiguration().set("mapreduce.output.basename", "ids_batch-"+ dateFormat.format(new Date()));
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 job.waitForCompletion(true);
	}
}
