import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BakeryProductsTotal {

  public static void main(String[] args) throws Exception {

    /*
     * Validate that two arguments were passed from the command line.
     */
    if (args.length != 3) {
      System.out.println("Usage: BakeryProductsTotal <goods dir> <items dir> <output dir>");
      System.exit(-1);
    }
    
    Job job = Job.getInstance();
    
    job.setJarByClass(BakeryProductsTotal.class);
    job.setJobName("Bakery products count");

    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,BakeryGoodsMapper.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,BakeryItemsMapper.class);
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
    job.setReducerClass(BakeryProductsTotalReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}

