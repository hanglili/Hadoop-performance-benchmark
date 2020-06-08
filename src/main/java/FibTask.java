import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FibTask {

  private static int fibonacci(int n) {
    if (n < 0) {
      System.out.println("Incorrect input");
      return 0;
    } else if (n == 1) {
      return 0;
    } else if (n == 2) {
      return 1;
    } else {
      return fibonacci(n - 1) + fibonacci(n - 2);
    }
  }

  public static class FibonacciMapper extends Mapper<Object, Text, Text, IntWritable> {
    private IntWritable durationKey = new IntWritable();
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String data = value.toString();
      String[] fields = data.split(",", -1);

      int duration = Integer.parseInt(fields[8]);
      durationKey.set(fibonacci(duration + 6));

      Text srcIpValue = new Text(fields[0]);
      context.write(srcIpValue, durationKey);
    }
  }

  public static class FibonacciReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iterator = values.iterator();
      int durationSum = 0;
      while (iterator.hasNext()) {
        durationSum += iterator.next().get();
      }
      int magicNumber = fibonacci((durationSum % 10) + 5);
      IntWritable output = new IntWritable(magicNumber);
      context.write(key, output);
    }
  }

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 2) {
      System.err.println("Usage: Fibonacci <input_file> <output_dir>");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "Fibonacci Task");
    job.setJarByClass(FibTask.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(FibonacciMapper.class);
    job.setReducerClass(FibonacciReducer.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    boolean status = job.waitForCompletion(true);
    int statusCode = status? 0 : 1;
    System.exit(statusCode);
  }

}

