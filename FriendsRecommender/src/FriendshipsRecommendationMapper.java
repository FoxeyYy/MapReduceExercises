import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FriendshipsRecommendationMapper extends Mapper<Text, Text, IntWritable, Friend> {
		
		@Override
		public void map(Text users, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] keyIDs = users.toString().split(",");
			int firstUser = Integer.valueOf(keyIDs[0]);
			int secondUser = Integer.valueOf(keyIDs[1]);
			int count = Integer.valueOf(value.toString());

			context.write(new IntWritable(firstUser), 
					new Friend(secondUser, count));
			
			context.write(new IntWritable(secondUser), 
					new Friend(firstUser, count));
		}

}
