import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FriendshipsRecommendationMapper extends Mapper<Text, Text, IntWritable, Text> {
		
		@Override
		public void map(Text users, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] keyIDs = users.toString().split(",");
			context.write(new IntWritable(Integer.valueOf(keyIDs[0])), 
					new Text(keyIDs[1] + "," + value));
			

		}

}
