import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FriendshipsCountMapper extends Mapper<Text, Text, FriendsKey, IntWritable> {
	
	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer friendsItr = new StringTokenizer(value.toString(), ",");
		List<Integer> friendsList = new ArrayList<>();
		
		while (friendsItr.hasMoreTokens()) {
			int friend = Integer.valueOf(friendsItr.nextToken());
			friendsList.add(friend);
			FriendsKey friendkey = new FriendsKey(Integer.valueOf(key.toString()), friend);
			context.write(friendkey, new IntWritable(0));
		}
		
		for (int i = 0; i < friendsList.size(); i++) {
			int friend = friendsList.get(i);
			for (int j = i + 1; j < friendsList.size(); j++) {
				int friend2 = friendsList.get(j);
				FriendsKey friendkey;
				if (friend < friend2) {
					friendkey = new FriendsKey(friend, friend2);
				} else {
					friendkey = new FriendsKey(friend2, friend);
				}
				context.write(friendkey, new IntWritable(1));
			}
		}
		

	}

}
