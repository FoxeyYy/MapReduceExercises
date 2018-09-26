import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FriendshipsCountMapper extends Mapper<Text, Text, Text, IntWritable> {
	
	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer friendsItr = new StringTokenizer(value.toString(), ",");
		List<Integer> friendsList = new ArrayList<>();
		
		while (friendsItr.hasMoreTokens()) {
			int friend = Integer.valueOf(friendsItr.nextToken());
			friendsList.add(friend);
			context.write(new Text(key + "," + friend), new IntWritable(0));
		}
		
		for (int friend: friendsList) {
			for (int friend2: friendsList) {
				if (friend != friend2) {
					context.write(new Text(friend + "," + friend2), new IntWritable(1));
				}
			}
		}
		

	}

}
