import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LoopTask {

  public static class LoopMapper extends Mapper<Object, Text, Text, FloatWritable> {
    private FloatWritable adRevenueKey = new FloatWritable();
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String data = value.toString();
      String[] fields = data.split(",", -1);

      float adRevenue = Float.parseFloat(fields[3]);
      adRevenueKey.set(adRevenue);

      int i = 0;
      while (i < 2500) {
        i++;
      }

      String subSrcIp = fields[0].substring(0, 7);
      Text srcIpValue = new Text(subSrcIp);
      context.write(srcIpValue, adRevenueKey);
    }
  }

  public static class LoopReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<FloatWritable> iterator = values.iterator();
      float count = 0.0f;
      while (iterator.hasNext()) {
        count += iterator.next().get();
      }
      int i = 0;
      while (i < 100000) {
        i++;
      }
      FloatWritable output = new FloatWritable(count);
      context.write(key, output);
    }
  }

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 2) {
      System.err.println("Usage: Loop <input_file> <output_dir>");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "Loop Task");
    job.setJarByClass(LoopTask.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);

    job.setMapperClass(LoopMapper.class);
    job.setReducerClass(LoopReducer.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    long start = new Date().getTime();
    boolean status = job.waitForCompletion(true);
    long end = new Date().getTime();
    System.out.println("Completed the job");
    System.out.println("Job execution time 1: " + (end - start));

    int statusCode = status? 0 : 1;
    System.exit(statusCode);
  }

}

