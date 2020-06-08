import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SortTask {

  public static class SortingMapper extends Mapper<Object, Text, FloatWritable, Text> {
    private FloatWritable adRevenueKey = new FloatWritable();
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String data = value.toString();
      String[] fields = data.split(",", -1);

      float adRevenue = Float.parseFloat(fields[3]);
      adRevenueKey.set(adRevenue);

      Text srcIpValue = new Text(fields[0]);
      context.write(adRevenueKey, srcIpValue);
    }
  }

  public static class SortingValueReducer extends Reducer<FloatWritable, Text, Text, FloatWritable> {
    public void reduce(FloatWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      for (Text t : values) {
        context.write(t, key);
      }
    }
  }

  public static class Partition extends Partitioner<FloatWritable, Text> {
    @Override
    public int getPartition(FloatWritable key, Text value, int numPartitions) {
      return (int) (key.get() * numPartitions);
    }
  }

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 2) {
      System.err.println("Usage: Sort <input_file> <output_dir>");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "Sorting Task");
    job.setJarByClass(SortTask.class);

    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(SortingMapper.class);
    job.setReducerClass(SortingValueReducer.class);
    job.setPartitionerClass(Partition.class);

//    job.setNumReduceTasks(10);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    boolean status = job.waitForCompletion(true);
    int statusCode = status? 0 : 1;
    System.exit(statusCode);
  }

}

