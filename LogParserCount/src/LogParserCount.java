import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

public class LogParserCount {

  public static void main(String[] args) throws Exception {

    /*
     * Validate that two arguments were passed from the command line.
     */
    if (args.length != 2) {
      System.out.println("Usage: LogParserCount <input dir> <output dir>");
      System.exit(-1);
    }
    
    Job job = new Job();
    
    job.setJarByClass(LogParserCount.class);
    job.setJobName("LogParserCount");

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

