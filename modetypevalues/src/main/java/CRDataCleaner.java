import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.gson.Gson;

public class CRDataCleaner {

  public static class CleaningMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private final Gson gson = new Gson();

    ArrayList<String> list = new ArrayList<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      if (value == null || value.toString().isEmpty())
        return;

      Battle battle = gson.fromJson(value.toString(), Battle.class);
      
      //if(battle.isAnyEmptyOrNull()) return;

      String str = battle.mode;

      if(!list.contains(str))
        list.add(str);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        for (String str : list) {
          context.write(new Text(str), NullWritable.get());
        }
    }
  }

  public static class CleaningReducer extends Reducer<Text, NullWritable, NullWritable, Text> 
  {
    @Override
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException 
    {
      context.write(NullWritable.get(), key);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "CRDataCleaner");
    //job.setNumReduceTasks(1);
    job.setJarByClass(CRDataCleaner.class);
    job.setMapperClass(CleaningMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);

    job.setReducerClass(CleaningReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);

    try {
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
    } catch (Exception e) {
      System.out.println(" bad arguments, waiting for 2 arguments [inputURI] [outputURI]");
      return;
    }

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}