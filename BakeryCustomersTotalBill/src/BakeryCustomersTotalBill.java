import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BakeryCustomersTotalBill {

  public static void main(String[] args) throws Exception {

    /*
     * Validate that two arguments were passed from the command line.
     */
    if (args.length != 2) {
      System.out.println("Usage: BakeryCustomersTotalBill <bakery data dir> <output dir>");
      System.exit(-1);
    }
    
    Job job1 = Job.getInstance();
    
    job1.setJarByClass(BakeryCustomersTotalBill.class);
    job1.setJobName("Join on Goods & Items");

    MultipleInputs.addInputPath(job1, new Path(args[0] + "/goods.csv"), TextInputFormat.class,BakeryCustomersTotalBillGoodsMapper.class);
    MultipleInputs.addInputPath(job1, new Path(args[0] + "/items.csv"), TextInputFormat.class, BakeryCustomersTotalBillItemsMapper.class);
    FileOutputFormat.setOutputPath(job1, new Path("./aux1"));
        
    job1.setReducerClass(BakeryCustomersTotalBillReceiptGoodsItemsReducer.class);
    
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    
    job1.waitForCompletion(true);
    
    Job job2 = Job.getInstance();
    
    job2.setJarByClass(BakeryCustomersTotalBill.class);
    job2.setJobName("Join on Receipts & Prices");

    MultipleInputs.addInputPath(job2, new Path("./aux1/"), TextInputFormat.class,BakeryCustomersTotalBillPricesMapper.class);
    MultipleInputs.addInputPath(job2, new Path(args[0] + "/receipts.csv"), TextInputFormat.class, BakeryCustomersTotalBillReceiptMapper.class);
    FileOutputFormat.setOutputPath(job2, new Path("./aux2"));
        
    job2.setReducerClass(BakeryCustomersTotalBillReceiptBillReducer.class);
    
    job2.setMapOutputValueClass(Text.class);
    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(FloatWritable.class);
    
    job2.waitForCompletion(true);
    FileSystem fs = FileSystem.get(new Configuration());
    fs.delete(new Path("./aux1"), true);
    
    Job job3 = Job.getInstance();
    
    job3.setJarByClass(BakeryCustomersTotalBill.class);
    job3.setJobName("Final join for customers and bills");

    MultipleInputs.addInputPath(job3, new Path("./aux2/"), TextInputFormat.class,BakeryCustomersTotalBillTotalBillPerReceiptMapper.class);
    MultipleInputs.addInputPath(job3, new Path(args[0] + "/customers.csv"), TextInputFormat.class, BakeryCustomersTotalBillCustomerMapper.class);
    FileOutputFormat.setOutputPath(job3, new Path(args[1]));
        
    job3.setReducerClass(BakeryCustomersTotalBillReceiptFinalReducer.class);
    
    job3.setOutputKeyClass(IntWritable.class);
    job3.setOutputValueClass(Text.class);
    
    boolean success = job3.waitForCompletion(true);
    fs.delete(new Path("./aux2"), true);

    System.exit(success ? 0 : 1);
  }
}

