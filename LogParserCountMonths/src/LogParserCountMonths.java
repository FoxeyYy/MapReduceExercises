import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

public class LogParserCountMonths {

  public static void main(String[] args) throws Exception {

    /*
     * Validate that two arguments were passed from the command line.
     */
    if (args.length != 2) {
      System.out.println("Usage: LogParserCountMonths <input dir> <output dir>");
      System.exit(-1);
    }
    
    Job job = Job.getInstance();
    
    job.setJarByClass(LogParserCountMonths.class);
    job.setJobName("LogParserCount per Month");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    job.setInputFormatClass(SequenceFileInputFormat.class);
    
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.setMapperClass(LogParserMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(LogParserReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}

