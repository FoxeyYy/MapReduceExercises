import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class LogParserFileTreatment {

  public static void main(String[] args) throws Exception {

    /*
     * Validate that two arguments were passed from the command line.
     */
    if (args.length != 2) {
      System.out.printf("Usage: LogParser <input dir> <output dir>\n");
      System.exit(-1);
    }
    
    Job job = new Job();
    
    job.setJarByClass(LogParserFileTreatment.class);
    job.setJobName("LogParser");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(LogParserMapper.class);
    job.setNumReduceTasks(0);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LogRecord.class);
    
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
    SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
    
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}

