import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BakeryCustomersTotalBillReceiptGoodsItemsReducer extends Reducer<Text, Text, IntWritable, FloatWritable> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		float productPrice = 0.0f;
		List<Integer> receipts = new ArrayList<>();

		for (Text line : values) {
			String[] elements = line.toString().split(",");
			if (elements[0].equals("A")) {
				productPrice = Float.valueOf(elements[1]);
			} else {
				receipts.add(Integer.valueOf(elements[1]));
			}
		}

		for (Integer receipt: receipts) {
			context.write(new IntWritable(receipt), new FloatWritable(productPrice));
		}
		
	}

}