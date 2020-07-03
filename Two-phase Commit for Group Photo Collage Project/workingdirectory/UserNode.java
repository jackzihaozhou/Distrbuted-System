import java.io.*;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.channels.*;
import java.util.concurrent.*;

public class UserNode implements ProjectLib.MessageHandling {
	public final String myId;
	private static ProjectLib PL;
	private static final String log_path = "log.txt";
	private static ConcurrentMap<String, String> fileLock = new ConcurrentHashMap<>();	
	public UserNode( String id ) {
		myId = id;
	}
	
	/**
	 * A callback method that receives messages from the Server. This method distinguish the
	 * opcode the message, and invoke different method correspondingly.
	 * 
	 * @param msg - the message received from the Server
	 * @return whether the message is successfully received and processed
	 */
	
	public boolean deliverMessage( ProjectLib.Message msg ) {
		Message msgContent = unpackMessage(msg.body);

		int opt = msgContent.optcode;
	
		if(opt==1) {
			handleQuery(msg,msgContent);
		}
		if(opt==3) {
			handleFinalDecision(msg,msgContent);
		}
		
		return true;
	}
	
	/**
	 * If the Server asks to abort, the user releases all its related source files. 
	 * Otherwise, delete corresponding local image and release the lock.
	 * No matter agree or abort, send back ACK to server 
	 * @param msg - the message received from the Server
	 * @param msgContent - the Message object from the Server
	 */
	
	public void handleFinalDecision(ProjectLib.Message msg, Message msgContent){
		String[] sources = msgContent.sources;
		String filename = msgContent.filename;
		Queue<String> ThisNodeFile = getThisNodeFile(sources);
		if(msgContent.action == true) {
			for (String my_file : ThisNodeFile) {
				
				if (fileLock.containsKey(my_file)&& (fileLock.get(my_file).equals(filename)) ){
					fileLock.remove(my_file);
					File deletefile = new File(my_file);
					if(deletefile.exists()){
						deletefile.delete();
						fileLock.remove(my_file);
						String log_str = my_file + ":" + filename + ":" + "DELETE";
						addToLog(log_str,log_path);
						PL.fsync();			
					}
				}
			}
		}
		else {
			String[] my_sources = getMySources(sources);

			for (String my_file : my_sources) {
				
				if (fileLock.containsKey(my_file)  ){
					if(fileLock.get(my_file).equals(filename)){
						String log_str = my_file + ":" + filename + ":" + "UNLOCK";
						addToLog(log_str,log_path);
						PL.fsync();
						fileLock.remove(my_file);
					}
				}
				else {
				}
			}
		}
		Message ACK = new Message(4, filename, myId, msg.addr);
		byte[] byte_ACK_msg = packMessage(ACK);
		ProjectLib.Message ACK_msg = new ProjectLib.Message(msg.addr, byte_ACK_msg);
		PL.sendMessage(ACK_msg);
	}
	/**
	 * Sends the vote agreement message to the server. If the user node approves to
	 * commit, it sends a message with agreement. Otherwise, it sends a message indicating refuse.
	 * 
	 * @param msg - the message received from the Server
	 * @param msgContent - the Message object from the Server
	 */
	public void handleQuery(ProjectLib.Message msg, Message msgContent){
		byte[] img = msgContent.img;
		String filename = msgContent.filename;
		String[] sources = msgContent.sources;			
		String addr;
		String comp_file;

		String[] my_sources = getMySources(sources);
		boolean agree = true;

		for(int i=0;i<sources.length;i++){
			String[] split = sources[i].split(":");
			addr = split[0];
			comp_file = split[1];
			if(addr.equals(myId)) {
				File file = new File(comp_file);
				if(!file.exists()){
					agree = false;
					break;
				}
				else if (fileLock.containsKey(comp_file)) {
					if (fileLock.get(comp_file) != filename) {
						agree = false;
						break;
					}
					else {
					}
				}
				else {
					String log_str = comp_file + ":" + filename + ":" + "LOCK";
					addToLog(log_str,log_path);
					PL.fsync();
					fileLock.put(comp_file, filename);
				}
			}		
		}

		if(PL.askUser(img,my_sources)) {
		}
		else {
			agree = false;
		}

		Message voteMsg = new Message(2, filename, myId, msg.addr);
		if (agree) {
			voteMsg.agree = true;
			// make sure all files are locked
			for (String my_file : my_sources) {
				if (!fileLock.containsKey(my_file)) {
					String log_str = my_file + ":" + filename + ":" + "LOCK";
					addToLog(log_str,log_path);
					PL.fsync();
					fileLock.put(my_file, filename);
				}
				else {
				}
			}
		}
		else {
			voteMsg.agree = false;
			for (String my_file : my_sources) {
				if (fileLock.containsKey(my_file)  ){
					if(fileLock.get(my_file).equals(filename)){						
						String log_str = my_file + ":" + filename + ":" + "UNLOCK";
						addToLog(log_str,log_path);
						PL.fsync();						
						fileLock.remove(my_file);
					}
				}
				else {
					
				}
			}
		}
		byte[] vote_msg_body = packMessage(voteMsg);			
		ProjectLib.Message vote_msg = new ProjectLib.Message(msg.addr, vote_msg_body);
		PL.sendMessage(vote_msg);
	}
	
	/**
	 * Converts the received message from byte array format into a Message object.
	 * 
	 * @param msgBytes - the byte array of the message
	 * @return msg - the Message object
	 */
	
	public static Message unpackMessage(byte[] msgBytes) {

		Message msg = null;
		ByteArrayInputStream byteArrayIn = new ByteArrayInputStream(msgBytes);

		try {
			ObjectInputStream in = new ObjectInputStream(byteArrayIn);
			msg = (Message) in.readObject();
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			byteArrayIn.close();
		} catch (IOException e) {
		}
		
		return msg;
		
	}
	
	/**
	 * Converts the message to be sent from a Message object to byte array.
	 * 
	 * @param msg - the Message object
	 * @return msgBytes - message byte array
	 */
	
	public static byte[] packMessage(Message msg) {
		
		byte[] msgBytes = null;

		ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream();
		try {
			ObjectOutputStream out = new ObjectOutputStream(byteArrayOut);
			out.writeObject(msg);
			out.flush();
			msgBytes = byteArrayOut.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			byteArrayOut.close();
		} catch (IOException e) {
		}
		
		return msgBytes;
	}
	public Queue<String> getThisNodeFile(String[] sources) {
		String addr;
		String cop_file;
		Queue<String> thisNodeFile = new LinkedList<String>();
		for(int i=0;i<sources.length;i++){

			String[] split = sources[i].split(":");
			addr = split[0];
			cop_file = split[1];
			
			if(addr.equals(myId)){
				thisNodeFile.add(cop_file);
			}
		}
		return(thisNodeFile);
	}
	public String[] getMySources(String[] sources) {
		String addr;
		String comp_file;
		int file_num = 0;
		for(int i=0;i<sources.length;i++){
			String[] split = sources[i].split(":");
			addr = split[0];
			comp_file = split[1];
			if(addr.equals(myId)) {
				file_num++;
			}			
		}
		String[] my_sources = new String[file_num];
		file_num=0;
		for(int i=0;i<sources.length;i++){
			String[] split = sources[i].split(":");
			addr = split[0];
			comp_file = split[1];
			if(addr.equals(myId)) {
				my_sources[file_num] = comp_file;
				file_num++;
			}			
		}
		return(my_sources);
	}
	/**
	 * println log_str in log_path
	 * @log_str - the String to println 
	 * @log_path - the path of log document
	 */
	public void addToLog(String log_str, String log_path) {
		try {
			RandomAccessFile log_file = new RandomAccessFile(log_path, "rws");
			long fileLength = log_file.length();
			log_file.seek(fileLength);

			String str=log_str+"\n";			
			log_file.write(str.getBytes());
			log_file.close();
		}
		catch(Exception e) {
		}
	}
	
	/**
	 * Recovers from the last failure of the UserNode and the lock status of the UserNode. 
	 * it will reimplement the operation in log in order and finally reach the same status as before the crash.
	 * @log_path the log path of this node 
	 */
	
    public static void nodeRecoverLogByLines(String log_path) {
        File file = new File(log_path);
		if(file.exists()){
	        BufferedReader reader = null;
	        try {
	            reader = new BufferedReader(new FileReader(file));
	            String tempString = null;
	            while ((tempString = reader.readLine()) != null) {
	            	if(tempString.length()!=0) {
	            		String[] split = tempString.split(":");
	            		String my_file = split[0];
	            		String filename = split[1];
	            		if(split[2].equals("LOCK")) {
	            			fileLock.put(my_file, filename);
	            		}
	            		else if(split[2].equals("UNLOCK")) {
	            			
	        				if (fileLock.containsKey(my_file)){
	        					if(fileLock.get(my_file).equals(filename)){
	        						fileLock.remove(my_file);
	        					}
	        					else {
	        					}
	        				}
	        				else {
	        				}
	            		}
	            		else if(split[2].equals("DELETE")) {
	        				if (fileLock.containsKey(my_file)){
	        					if(fileLock.get(my_file).equals(filename)){
	        						fileLock.remove(my_file);
	        						File deletefile = new File(my_file);
	        						if(deletefile.exists()){
	        							deletefile.delete();
	        						}
	        					}
	        				}
	            		}
	            		else {
	            		}
	            	}
	            }
	            reader.close();
	        } 
	        catch (IOException e) {
	        }
		}
    }
	public static void main ( String args[] ) throws Exception {
		if (args.length != 2) throw new Exception("Need 2 args: <port> <id>");
		UserNode UN = new UserNode(args[1]);
		PL = new ProjectLib( Integer.parseInt(args[0]), args[1], UN );
		nodeRecoverLogByLines(log_path);
		while (true) {
			ProjectLib.Message msg = PL.getMessage();
			String body = new String(msg.body);
		}

	}
}
