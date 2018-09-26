import java.util.Comparator;


public class Friend {
	
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
	
	private final int id;
	private final int friendshipsCount;
	
	public Friend(int id, int friendshipsCount) {
		this.id = id;
		this.friendshipsCount = friendshipsCount;
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
