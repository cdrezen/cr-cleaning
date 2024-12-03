import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;

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

  public static class BattleKey implements WritableComparable<BattleKey> {

    public String key, date;

    public BattleKey() {

    }

    public BattleKey(String u1, String u2, int  round, String date) {
      this.key = round + "_" + (u1.compareTo(u2) < 0 ? u1 + u2 : u2 + u1);
      this.date = date;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.key = in.readUTF();
      this.date = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(key);
      out.writeUTF(date);
    }

    @Override
    public int compareTo(BattleKey o) {
      int keyCompare = this.key.compareTo(o.key);
      if (keyCompare != 0) {
        return keyCompare;
      }
      Instant instant1 = Instant.parse(this.date);
      long secondsSinceEpoch1 = instant1.getEpochSecond();
      Instant instant2 = Instant.parse(o.date);
      long secondsSinceEpoch2 = instant2.getEpochSecond();
      if (Math.abs(secondsSinceEpoch1 - secondsSinceEpoch2) <= 10)
        return 0;
      else
        return keyCompare;
    }

  }

  public static class CleaningMapper extends Mapper<LongWritable, Text, BattleKey, Battle> {

    private final Gson gson = new Gson();;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      if (value == null || value.toString().isEmpty())
        return;

      Battle battle = gson.fromJson(value.toString(), Battle.class);
      String u1 = battle.players.get(0).utag;
      String u2 = battle.players.get(1).utag;
      BattleKey battleKey = new BattleKey(u1, u2, battle.round, battle.date);
      context.write(battleKey, battle);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
  }

  public static class CleanGrouping extends WritableComparator {

    public CleanGrouping() {
      super(BattleKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      BattleKey ka = (BattleKey) a;
      BattleKey kb = (BattleKey) b;
      return ka.compareTo(kb);
    }
  }

  public static class CleaningCombiner extends Reducer<BattleKey, Battle, Text, Text> {
    @Override
    public void reduce(BattleKey key, Iterable<Battle> values,
        Context context) throws IOException, InterruptedException {

    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "CRDataCleaner");
    // job.setNumReduceTasks(1);
    job.setJarByClass(CRDataCleaner.class);
    job.setMapperClass(CleaningMapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    // job.setMapOutputValueClass(City.class);

    job.setGroupingComparatorClass(CleanGrouping.class);
    job.setCombinerClass(CleaningCombiner.class);
    // job.setReducerClass(TopKReducer.class);
    // job.setOutputKeyClass(City.class);
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