import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class FriendsKey implements WritableComparable<FriendsKey> {

	private int firstFriend, secondFriend;
	
	public FriendsKey() {
		
	}
	
	public FriendsKey(int first, int second) {
		firstFriend = first;
		secondFriend = second;
	}
	
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		firstFriend = in.readInt();
		secondFriend = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(firstFriend);
		out.writeInt(secondFriend);
	}

	@Override
	public int compareTo(FriendsKey o) {
		int firstComp = (new Integer(firstFriend)).compareTo(o.getFirstFriend());
		
		return firstComp != 0 ? firstComp : (new Integer(secondFriend)).compareTo(o.getSecondFriend());
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + firstFriend;
		result = prime * result + secondFriend;
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof FriendsKey))
			return false;
		FriendsKey other = (FriendsKey) obj;
		if (firstFriend != other.firstFriend)
			return false;
		if (secondFriend != other.secondFriend)
			return false;
		return true;
	}
	

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return firstFriend + "," + secondFriend;
	}

	/**
	 * @return the firstFriend
	 */
	public int getFirstFriend() {
		return firstFriend;
	}

	/**
	 * @return the secondFriend
	 */
	public int getSecondFriend() {
		return secondFriend;
	}


}
