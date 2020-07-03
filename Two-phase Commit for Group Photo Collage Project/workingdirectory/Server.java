import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.io.*;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.file.Files;
import java.nio.file.OpenOption;

/* Skeleton code for Server */

public class Server implements ProjectLib.CommitServing {
	
    static Object Lock = new Object();
	private static ProjectLib PL;
	private static final long time_threshold = 3000;
	private static List<Message> commitReply = Collections.synchronizedList(new ArrayList<Message>());
	private static List<Message> ackReply = Collections.synchronizedList(new ArrayList<Message>());
	
	/**
	 * Server callback when new collage is to be committed.
	 * A call to this function should start a two-phase commit operation.
	 * 
	 * @filename - name of candidate commit image
	 * @img - byte array of candidate commit image
	 * @sources - string array indicating the contributing files in "source_node:filename" format
	 */
	public void startCommit( String filename, byte[] img, String[] sources ) {

		mkdir("log");
		
		String log_path = "log/log_"+(filename.split("\\."))[0]+".txt";
		
		logBeforePhaseOne(filename,img,sources,log_path);
		
		boolean decision = phaseOne(filename,img,sources);
		
		logBetweenOneAndTwo(decision,filename,log_path);
		
		phaseTwo(decision,filename,img,sources);
		
		logCommit(filename,log_path);
	}	
	
	/**
	 * Performs Phase One procedure
	 * @filename - name of candidate commit image
	 * @img - byte array of candidate commit image
	 * @sources - string array indicating the contributing files in "source_node:filename" format
	 * @return decision - the commit decision of this turn of vote 
	 */
	public static boolean phaseOne(String filename, byte[] img, String[] sources) {
		int query_opt = 1;
		
		Set<String> NodeSet = new HashSet<String>();

		NodeSet = getNode(sources);

		for(String addr:NodeSet) {		
			Message msgContent = new Message(1,filename,"Server", addr);
			msgContent.img = img;
			msgContent.sources = sources;
			byte[] msg_body = packMessage(msgContent);
			ProjectLib.Message msg = new ProjectLib.Message(addr, msg_body);
			PL.sendMessage( msg );
		}
		
		int sent_node_num = NodeSet.size();
		long initTime = System.currentTimeMillis();
		long currTime = System.currentTimeMillis();
		
		boolean decision = true;
		int response = 0;
		long start_time = System.currentTimeMillis();
		long cur_time = System.currentTimeMillis();
		while (response < sent_node_num) {

			synchronized (commitReply) {
				Iterator<Message> it=commitReply.iterator();
				while(it.hasNext()) {
				
					Message msg_vote=it.next();

					
					if(msg_vote.filename.equals(filename) && msg_vote.agree && NodeSet.contains(msg_vote.sender)) {
						response++;
						it.remove();
					}
					else if(msg_vote.filename.equals(filename) && (!msg_vote.agree) && NodeSet.contains(msg_vote.sender)){
						response++;
						decision = false;
						it.remove();
					}
				}
			}	
			currTime = System.currentTimeMillis();
			if((currTime-start_time)>time_threshold) {
				decision = false;
				break;
			}
		}
		return(decision);
		
	}
	/**
	 * Performs Phase Two procedure
	 * @decision - the decision return by Phase One
	 * @filename - name of candidate commit image
	 * @img - byte array of candidate commit image
	 * @sources - string array indicating the contributing files in "source_node:filename" format
	 */
	public static void phaseTwo(boolean decision,String filename, byte[] img,String[] sources) {
		Set<String> NodeSet = getNode(sources);

		if(decision) {
			try {
				RandomAccessFile collage = new RandomAccessFile(filename, "rws");
				collage.write(img);
				collage.close();
			}
			catch(Exception e) {
				System.err.println(e);
			}
			
			for(String addr:NodeSet) {		
				Message msgContent = new Message(3,filename,"Server", addr);
				msgContent.sources = sources;
				msgContent.action = true;
				byte[] msg_body = packMessage(msgContent);
				ProjectLib.Message msg = new ProjectLib.Message(addr, msg_body);
				PL.sendMessage( msg );
			}
			
		}
		else {

			for(String addr:NodeSet) {		
				Message msgContent = new Message(3,filename,"Server", addr);
				msgContent.sources = sources;
				msgContent.action = false;
				byte[] msg_body = packMessage(msgContent);
				ProjectLib.Message msg = new ProjectLib.Message(addr, msg_body);
				PL.sendMessage( msg );
			}
		
		}
		
		Set<String> receiveACKNodeQueue = receiveACKMessages(NodeSet,filename);
		
		if (receiveACKNodeQueue.size() == NodeSet.size()) {
			return;
		}
		
		Set<String> timeoutNodes =  new HashSet<String>();
		for (String node : NodeSet) {
			timeoutNodes.add(node);
		}
		timeoutNodes.removeAll(receiveACKNodeQueue);

		while (timeoutNodes.size() != 0) {
			for (String addr : timeoutNodes) {
				System.out.println("send to "+addr+" decision again");
				if (decision) {
					Message msgContent = new Message(3,filename,"Server", addr);
					msgContent.sources = sources;
					msgContent.action = true;
					byte[] msg_body = packMessage(msgContent);
					ProjectLib.Message msg = new ProjectLib.Message(addr, msg_body);
					PL.sendMessage( msg );
				}
				else {
					Message msgContent = new Message(3,filename,"Server", addr);
					msgContent.sources = sources;
					msgContent.action = false;
					byte[] msg_body = packMessage(msgContent);
					ProjectLib.Message msg = new ProjectLib.Message(addr, msg_body);
					PL.sendMessage( msg );
				}
			}
			timeoutNodes.removeAll(receiveACKMessages(NodeSet,filename));
		}
		
	}
	
	/**
	 * Recieve ACK from given set of nodes, used by Phase Two process
	 * @NodeSet - a set of nodes in this round of voting
	 * @filename - name of candidate commit image
	 * @return - string array indicating the contributing files in "source_node:filename" format
	 */
	public static Set<String> receiveACKMessages(Set<String> NodeSet,String filename) {
		
		int node_num = NodeSet.size();
		Set<String> replyNodeSet = new HashSet<String>();
		long initTime = System.currentTimeMillis();

		
		while (replyNodeSet.size() < node_num) {

			synchronized (ackReply) {
				Iterator<Message> it=ackReply.iterator();
				while(it.hasNext()) {
				
					Message msg_ack=it.next();

					
					if(msg_ack.filename.equals(filename) && NodeSet.contains(msg_ack.sender)) {

						replyNodeSet.add(msg_ack.sender);
					}
				}
			}
	
			if ((System.currentTimeMillis() - initTime) > time_threshold) {
				break;
			}
		}
		
		return replyNodeSet;
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
		} 
		catch (IOException e) {
		}
		
		return msgBytes;
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
		} 
		catch (IOException e) {
		}
		
		return msg;
		
	}

	/**
	 * Given sources handed in startCommit, return a set of nodes in this round of voting
	 * @sources - string array indicating the contributing files in "source_node:filename" format
	 * @return - a set of nodes in this round of voting
	 */
	public static Set<String> getNode(String[] sources) {
		String addr;
		String cop_file;
		Set<String> NodeSet = new HashSet<>();
		for(int i=0;i<sources.length;i++){

			String[] split = sources[i].split(":");
			addr = split[0];
			cop_file = split[1];
			
			if(!NodeSet.contains(addr)){
				NodeSet.add(addr);
			}
		}
		return(NodeSet);
	}
	/**
	 * println log_str in log_path
	 * @log_str - the String to println 
	 * @log_path - the path of log document
	 */
	public static void addToLog(String log_str, String log_path) {
		try {
			RandomAccessFile log_file = new RandomAccessFile(log_path, "rws");
			long fileLength = log_file.length();
			log_file.seek(fileLength);

			String str=log_str+"\n";			
			log_file.write(str.getBytes());
			log_file.close();
		}
		catch(Exception e) {
			System.err.println(e);
		}
	}
	/**
	 * print log_str in log_path
	 * @log_str - the String to print
	 * @log_path - the path of log document
	 */
	public static void appendToLog(String log_str, String log_path) {
		try {
			RandomAccessFile log_file = new RandomAccessFile(log_path, "rws");
			long fileLength = log_file.length();
			log_file.seek(fileLength);

			String str=log_str;			
			log_file.write(str.getBytes());
			log_file.close();
		}
		catch(Exception e) {
			System.err.println(e);
		}
	}
	/**
	 * temporarily store image in log folder,
	 * @img - byte array of candidate commit image
	 * @log_imgpath - the path of log image
	 */
	public static synchronized void logCommitImage(String log_imgpath, byte[] img) {
		
		try {
			RandomAccessFile log_collage = new RandomAccessFile(log_imgpath, "rws");
			log_collage.write(img);
			log_collage.close();
		}
		catch(Exception e) {
		}
		
	}
	
	/**
	 * Recover the Server environment from the last failure. If the last failure crash at Phase One, after recover, the server will send abort to all nodes. 
	 * If the last failure crash at Phase Two, after recover continue Phase Two based on the decision in Phase One.
	 * @log_folder_path - the path of log folder
	 */
    public static void serverRecoverLogByLines(String log_folder_path) {
    	
        File logFolder = new File(log_folder_path);

		File[] logFiles = logFolder.listFiles();
		
		if (logFiles == null || logFiles.length == 0) {
			return;
		}
        
		for (File file : logFiles) {
			if(!file.getName().startsWith("log")) {
				continue;
			}
            String line;
            String filename=null;
            boolean PhaseOneisDone = false;
            boolean decision = false;
            boolean CommitisDone = false;
            String[] sources=null;
            byte[] img=null;
	        BufferedReader reader = null;
	        try {
	            reader = new BufferedReader(new FileReader(file));

	            while ((line = reader.readLine()) != null) {
	            	if(line.length()!=0) {
	    				if (line.startsWith("Filename/")) {
	    					filename = line.split("/")[1];
	    				}
	    				else if (line.startsWith("Source/")) {
	    					String temp = line.split("/")[1];
	    					sources = temp.split(",");
	    				}
	    				else if (line.startsWith("Decision/")){
	    					String temp = line.split("/")[1];
	    					if(temp.equals("true")) {
	    						decision = true;
	    					}
	    				}
	    				else if (line.startsWith("PhaseOneDone")){
	    					PhaseOneisDone = true;
	    				}
	    				else if (line.startsWith("Commit")){
	    					CommitisDone=true;
	    				}
	    				else {
	    				}
	            	}

	            }
	            reader.close();
				if (CommitisDone) {
					continue;
				}
				if (PhaseOneisDone) {
			        try {
			            File file1 = new File("log/imglog_"+filename);
			            img = Files.readAllBytes(file1.toPath());
			        } catch (IOException e) {
			            e.printStackTrace();
			        }
			        if(filename!=null&&img!=null&&sources!=null) {
			        	phaseTwo(decision,filename, img,sources);
			        	addToLog("Commit",file.getPath());
			        	PL.fsync();
			        }
			        else {
			        }
				}
				else {
					addToLog("Decision/Rfalse",file.getPath());
					addToLog("PhaseOneDone",file.getPath());
					PL.fsync();
					
					// perform Phase II
					if(filename!=null&&sources!=null) {
						phaseTwo(false,filename, img,sources);
					}
					else {
					}

					addToLog("Commit",file.getPath());
					PL.fsync();
					
				}
				
				
	        } 
	        catch (IOException e) {
	            e.printStackTrace();
	        }
		}
    }
	/**
	 * Given a path and create a folder
	 * @path - the path of the folder we want to create
	 */
    public static void mkdir(String path) {
		File file=new File(path);
		if(!file.exists()){
			file.mkdir();
			PL.fsync();
		}
    }
	/**
	 * Log process before Phase One
	 * @filename - name of candidate commit image
	 * @img - byte array of candidate commit image
	 * @sources - string array indicating the contributing files in "source_node:filename" format
	 * @log_path - the path of log document
	 */
    public static void logBeforePhaseOne(String filename, byte[] img, String[] sources, String log_path) {

		addToLog("Filename/"+filename,log_path);
		appendToLog("Source/",log_path);
		for(int i=0;i<sources.length;i++){
			appendToLog(sources[i],log_path);
			if(i!=sources.length-1) {
				appendToLog(",",log_path);
			}
			else {
				appendToLog("\n",log_path);
			}
		}
		logCommitImage("log/imglog_"+filename,img);
		PL.fsync();
    }
	/**
	 * Log process between Phase One and Phase Two
	 * @decision - the decision return by Phase One
	 * @filename - name of candidate commit image
	 * @log_path - the path of log document
	 */
    public static void logBetweenOneAndTwo(boolean decision,String filename,String log_path) {
		if(!decision) {
			File fileimg= new File("log/imglog_"+filename);
			if(fileimg.exists()){
				fileimg.delete();
				PL.fsync();
			}
		}
		
		addToLog("Decision/"+decision,log_path);
		addToLog("PhaseOneDone",log_path);
		PL.fsync();
    }
	/**
	 * Log commit after Phase Two
	 * @filename - name of candidate commit image
	 * @log_path - the path of log document
	 */
    public static void logCommit(String filename,String log_path) {
		addToLog("Commit",log_path);
		File fileimg= new File("log/imglog_"+filename);
		if(fileimg.exists()){
			fileimg.delete();
		}
		PL.fsync();
    }
	/**
	 * A helper class used by the Server as a message handler, which is called when a message is delivered to the Server.
	 */
	private static class ServerMessageHandler implements ProjectLib.MessageHandling {

		public synchronized boolean deliverMessage(ProjectLib.Message msg) {

			byte[] msgBytes =  msg.body;
			
			Message msgContent = unpackMessage(msgBytes);
			if(msgContent.optcode == 2) {
				commitReply.add(msgContent);		
			}
			else if(msgContent.optcode == 4) {
				ackReply.add(msgContent);		
			}
			return(true);
		}

	}
	
	public static void main( String args[] ) throws Exception {
		if (args.length != 1) throw new Exception("Need 1 arg: <port>");
		Server srv = new Server();
		PL = new ProjectLib( Integer.parseInt(args[0]), srv,new ServerMessageHandler());
		serverRecoverLogByLines("log");
	}
}

