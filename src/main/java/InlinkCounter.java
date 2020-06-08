import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InlinkCounter {

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
        urlOut.set(url);
        context.write(urlOut, one);
      }
    }
  }

  public static class SumReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
    public void reduce(Text key, Iterable<IntWritable> ones, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iterator = ones.iterator();
      float count = 0.0f;
      while (iterator.hasNext()) {
        count += (float) iterator.next().get();
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
      System.err.println("Usage: InlinkCount <input_file> <output_dir>");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "Inlink Count");
    job.setJarByClass(InlinkCounter.class);
    job.setMapperClass(TokenizeMapper.class);
    job.setReducerClass(SumReducer.class);
//    job.setNumReduceTasks(20);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    boolean status = job.waitForCompletion(true);
    System.out.println("Completed the job");
    if (status) {
      System.exit(0);
    } else {
      System.exit(1);
    }
  }

}
