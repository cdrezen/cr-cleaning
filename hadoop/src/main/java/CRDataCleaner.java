import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.util.Comparator;

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

    public String key;

    public long seconds;

    public BattleKey() {

    }

    public BattleKey(Battle battle) {
      String u1 = battle.players.get(0).utag;
      String u2 = battle.players.get(1).utag;
      this.key = battle.round + u1 + u2;
      this.seconds = Instant.parse(battle.date).getEpochSecond();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.key = in.readUTF();
      this.seconds = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(this.key);
      out.writeLong(this.seconds);
    }

    @Override
    public int compareTo(BattleKey o) {
      int keyCompare = this.key.compareTo(o.key);
      if (keyCompare != 0) {
        return keyCompare;
      }

      if (Math.abs(this.seconds - o.seconds) <= 10)
        return 0;
      else
        return 1;
    }

  }

  public static class CleaningMapper extends Mapper<LongWritable, Text, BattleKey, Battle> {

    private final Gson gson = new Gson();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      if (value == null || value.toString().isEmpty())
        return;

      Battle battle = gson.fromJson(value.toString(), Battle.class);

      if(battle.isAnyEmptyOrNull()) return;

      battle.players.sort(Comparator.comparing(p -> p.utag));

      BattleKey battleKey = new BattleKey(battle);
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

  public static class CleaningCombiner extends Reducer<BattleKey, Battle, BattleKey, Battle> 
  {
    @Override
    public void reduce(BattleKey key, Iterable<Battle> values, Context context) throws IOException, InterruptedException 
    {
      Battle b = values.iterator().next();
      context.write(key, b);
    }
  }

  public static class CleaningReducer extends Reducer<BattleKey, Battle, NullWritable, Text> 
  {
    private final Gson gson = new Gson();

    @Override
    public void reduce(BattleKey key, Iterable<Battle> values, Context context) throws IOException, InterruptedException 
    {
      Battle b = values.iterator().next();

      //restore data from key:
      b.date = Instant.ofEpochSecond(key.seconds).toString();
      Player p0 = b.players.get(0);
      Player p1 = b.players.get(1);
      String utags[] = key.key.split("#");
      p0.utag = '#' + utags[1];
      p1.utag = '#' + utags[2];
      //

      context.write(NullWritable.get(), new Text(gson.toJson(b, Battle.class)));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "CRDataCleaner");
    job.setNumReduceTasks(1);
    job.setJarByClass(CRDataCleaner.class);
    job.setMapperClass(CleaningMapper.class);
    job.setMapOutputKeyClass(BattleKey.class);
    job.setMapOutputValueClass(Battle.class);

    job.setCombinerKeyGroupingComparatorClass(CleanGrouping.class);
    job.setGroupingComparatorClass(CleanGrouping.class);
    job.setCombinerClass(CleaningCombiner.class);
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