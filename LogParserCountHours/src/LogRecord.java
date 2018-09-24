import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class LogRecord implements Writable {
	
	public LogRecord() {
		ip = "";
		date = "";
		time = "";
		statusCode = 0;
	}
	
	public LogRecord(String ip, String date, String time, short statusCode) {
		this.ip = ip;
		this.date = date;
		this.time = time;
		this.statusCode = statusCode;
	}

	private String ip;
	private String date;
	private String time;
	private short statusCode;

	@Override
	public void readFields(DataInput in) throws IOException {
		ip = in.readUTF();
		date = in.readUTF();
		time = in.readUTF();
		statusCode = in.readShort();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(ip);
		out.writeUTF(date);
		out.writeUTF(time);
		out.writeShort(statusCode);
	}

	/**
	 * @return the ip
	 */
	public String getIp() {
		return ip;
	}

	/**
	 * @return the date
	 */
	public String getDate() {
		return date;
	}

	/**
	 * @return the time
	 */
	public String getTime() {
		return time;
	}

	/**
	 * @return the statusCode
	 */
	public short getStatusCode() {
		return statusCode;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return ip + ", " + date + ", " + time
				+ ", " + statusCode;
	}

}
