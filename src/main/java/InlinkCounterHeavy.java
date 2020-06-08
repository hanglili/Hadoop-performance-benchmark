import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

public class InlinkCounterHeavy {

//  private static int fibonacci(int n) {
//    if (n < 0) {
//      System.out.println("Incorrect input");
//      return 0;
//    } else if (n == 1) {
//      return 0;
//    } else if (n == 2) {
//      return 1;
//    } else {
//      return fibonacci(n - 1) + fibonacci(n - 2);
//    }
//  }

  public static class TokenizeMapper extends Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      Text urlOut = new Text();
      IntWritable one = new IntWritable(1);

      String line = value.toString();
      Pattern p = Pattern.compile("(https?://[^\\s]+)");
      Matcher m = p.matcher(line);
      while (m.find()) {
        String url = m.group(1);
        int i = 0;
        while (i < 10000) {
          i++;
        }
        urlOut.set(url);
        context.write(urlOut, one);
      }
    }
  }

  public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> ones, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iterator = ones.iterator();
      int count = 0;
      while (iterator.hasNext()) {
        count += iterator.next().get();
      }
      int i = 0;
      while (i < 10000) {
        i++;
      }
      IntWritable output = new IntWritable(count);
      context.write(key, output);
    }
  }

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 2) {
      System.err.println("Usage: InlinkCountHeavy <input_file> <output_dir>");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "Inlink Count Heavy");
    job.setJarByClass(InlinkCounterHeavy.class);
    job.setMapperClass(TokenizeMapper.class);
    job.setReducerClass(SumReducer.class);
//    job.setNumReduceTasks(20);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    long start = new Date().getTime();
    boolean status = job.waitForCompletion(true);
    long end = new Date().getTime();
    System.out.println("Completed the job");
    System.out.println("Job execution time 1: " + (end - start));
    long startTime = job.getStartTime();
    long endTime = job.getFinishTime();
    System.out.println("Job execution time 2: " + (endTime - startTime));
    System.out.println("Job scheduling info: " + job.getSchedulingInfo());
    if (status) {
      System.exit(0);
    } else {
      System.exit(1);
    }
  }

}
