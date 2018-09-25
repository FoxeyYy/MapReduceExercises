import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BakeryClientsMultiShoppingCustomersMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer linesItr = new StringTokenizer(value.toString(), "\n");

		if (key.get() == 0 && linesItr.hasMoreTokens()) {
			linesItr.nextToken();	// Skip csv header
		}
		
		while (linesItr.hasMoreTokens()) {
			String[] line = linesItr.nextToken().split("\\s*,\\s*");
			context.write(new Text(line[0]), new Text("A," + line[2] + "," + line[1]));
		}
		

	}

}
