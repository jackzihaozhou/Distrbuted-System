import java.io.Serializable;


/**
 * @file Message.java
 * Represents the content of a message between the Server and the User Node.
 */
public class Message implements Serializable {
	final int optcode;
	String filename;
	final String sender;
	final String receiver;
	boolean agree = false;
	boolean action = false;
	byte[] img;
	String[] sources;
	public Message(int optcode, String filename, String sender, String receiver) {
		this.filename = filename;
		this.optcode = optcode;
		this.sender = sender;
		this.receiver = receiver;
	}
}