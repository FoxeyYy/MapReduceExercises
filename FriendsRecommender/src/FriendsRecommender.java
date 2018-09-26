import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FriendsRecommender {

  public static void main(String[] args) throws Exception {

    /*
     * Validate that two arguments were passed from the command line.
     */
    if (args.length != 2) {
      System.out.println("Usage: BakeryProductsTotal <friends dir> <output dir>");
      System.exit(-1);
    }
    
    Job job1 = Job.getInstance();
    
    job1.setJarByClass(FriendsRecommender.class);
    job1.setJobName("Counting friendshisps");

    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path("./aux"));
    
    job1.setInputFormatClass(KeyValueTextInputFormat.class);
        
    job1.setMapperClass(FriendshipsCountMapper.class);
    job1.setReducerClass(FriendshipsCounterReducer.class);
    
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    
    job1.waitForCompletion(true);
    
    Job job2 = Job.getInstance();
    
    job2.setJarByClass(FriendsRecommender.class);
    job2.setJobName("Recommending friendshisps");

    FileInputFormat.addInputPath(job2, new Path("./aux"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    
    job2.setInputFormatClass(KeyValueTextInputFormat.class);
        
    job2.setMapperClass(FriendshipsRecommendationMapper.class);
    job2.setReducerClass(FriendshipsRecommendationReducer.class);
    
    job2.setMapOutputKeyClass(IntWritable.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    
    boolean success = job2.waitForCompletion(true);
    
    FileSystem fs = FileSystem.get(new Configuration());
    fs.delete(new Path("./aux"), true);
    
    System.exit(success ? 0 : 1);
  }
}

