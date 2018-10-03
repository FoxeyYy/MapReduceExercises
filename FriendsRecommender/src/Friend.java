import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.io.Writable;


public class Friend implements Writable {
	
	public static class FriendComparator implements Comparator<Friend> {

		@Override
		public int compare(Friend o1, Friend o2) {
			int firstGreater = -Integer.valueOf(o1.getFriendshipsCount()).compareTo(o2.getFriendshipsCount());
			
			if (firstGreater != 0) {
				return firstGreater;
			}
			
			return Integer.valueOf(o1.getId()).compareTo(o2.getId());
		}
		
	}
	
	private int id;
	private int friendshipsCount;
	
	public Friend() {
		
	}
	
	public Friend(Friend fr) {
		id = fr.getId();
		friendshipsCount = fr.getFriendshipsCount();
	}
	
	public Friend(int id, int friendshipsCount) {
		this.id = id;
		this.friendshipsCount = friendshipsCount;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		friendshipsCount = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeInt(friendshipsCount);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Friend [id=" + id + ", friendshipsCount=" + friendshipsCount
				+ "]";
	}

	/**
	 * @return the id
	 */
	public int getId() {
		return id;
	}

	/**
	 * @return the friendshipsCount
	 */
	public int getFriendshipsCount() {
		return friendshipsCount;
	}

}
