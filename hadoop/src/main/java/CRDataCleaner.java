import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.gson.Gson;

public class CRDataCleaner {

  public static class CleaningMapper extends Mapper<LongWritable, Text, Text, Battle> 
  {

    Gson gson = new Gson();

    @Override
    public void map(LongWritable i, Text txt, Context context) throws IOException, InterruptedException {

      if (i.get() == 0 || txt.toString().isEmpty())
        return;
      
      Battle b = gson.fromJson(txt.toString(), Battle.class);
      String u1 = b.players.get(0).utag;
      String u2 = b.players.get(1).utag;

      context.write(new Text(u1+u2), b);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      
    }
  }

  public static class CleaningCombiner extends Reducer<Text, Battle, Text, Text> 
  {
    @Override
    public void reduce(Text key, Iterable<Battle> values,
        Context context) throws IOException, InterruptedException 
    {
			
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "CRDataCleaner");
    // job.setNumReduceTasks(1);
    job.setJarByClass(CRDataCleaner.class);
    job.setMapperClass(CleaningMapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    //job.setMapOutputValueClass(City.class);
    job.setCombinerClass(CleaningCombiner.class);
    //job.setReducerClass(TopKReducer.class);
    //job.setOutputKeyClass(City.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);

    try 
    {
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
		} 
		catch (Exception e) {
			System.out.println(" bad arguments, waiting for 2 arguments [inputURI] [outputURI]");
			return;
		}

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}