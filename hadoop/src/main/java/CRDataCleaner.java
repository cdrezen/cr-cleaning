import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.UUID;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.gson.Gson;

public class CRDataCleaner {

  public static class CleaningMapper extends Mapper<LongWritable, Text, NullWritable, Battle> 
  {

    Gson gson = new Gson();

    @Override
    public void map(LongWritable i, Text txt, Context context) throws IOException, InterruptedException {

      if (i.get() == 0 || txt.toString().isEmpty())
        return;
      
      Battle d = gson.fromJson(txt.toString(), Battle.class);
      String u1 = d.players.get(0).utag;
      String u2 = d.players.get(1).utag;


    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      
    }
  }

  public static class CleaningReducer extends Reducer<Text, Battle, Text, Text> 
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
    //job.setCombinerClass(City.class);
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