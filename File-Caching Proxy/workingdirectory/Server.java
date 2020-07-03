/**
 * Server, a simple server for RPC caching
 * @author Zihao ZHOU
 * @andrewID zihaozho
 */
import java.io.*;
import java.net.MalformedURLException;
import java.rmi.registry.*;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.rmi.Naming;
import java.rmi.RemoteException;

public class Server extends UnicastRemoteObject implements Iserver {
	
	public static Map<String, Integer> map_path_version = Collections.synchronizedMap(new HashMap<String, Integer>());
    
    private static int listenport;  
    
    private static String fileroot = null; 
    static Object cacheLock = new Object();


    public Server(String fileroot) throws RemoteException {
        Server.fileroot = fileroot;
    }

    private String getServerPath(String path) {
        StringBuilder sb = new StringBuilder(path);
        if (fileroot.charAt(fileroot.length()-1) != '/') {
            sb.insert(0, "/");
        }
        sb.insert(0, fileroot);

        return sb.toString();
    }

    /**
     * proxy use this to touch the file at server, to get the version, length and state(whether exist) 
     * @param path is the original path of a file
     * @return int[3], the aggregation of version, length and state corresponding to the original path, version :
     * version = version number
     * filelen = if existed, the total byte of this file 
     * state 0: file not exist, 1: file exist
     */
    public int[] touchFile(String path) throws RemoteException {
    	//we will get into this method only in server side 'r'    	
        String server_path = getServerPath(path); // get absolute path of the file on server 
        System.err.println("now try to touch: " + server_path + " at server side");
        File file = new File(server_path);	
        int version = 0;
        int fileLen;
        int state = 0;
		if(!file.exists()){
			fileLen = 0;
			state = 0;
		}
		else {
			state = 1;
			fileLen = (int)(file.length());
		}
		synchronized (cacheLock) {
			if(map_path_version.containsKey(path)) {
				version = map_path_version.get(path);
			}
			else {
				map_path_version.put(path,0);
				version = 0;
			}
		}	
		int[] ret = new int[3];
        ret[0] = version;
        ret[1] = fileLen;		
        ret[2] = state;	
        
        return ret;	
    }
    /**
     * update the version map at server
     * @param path is the original path of a file
     * @return the version number of the after updating correspoding to the given the original path
     */
    public synchronized int updateVersion(String path) throws RemoteException{
    	synchronized (cacheLock) {
	    	int new_version;
			if(map_path_version.containsKey(path)) {
				new_version = map_path_version.get(path);
				new_version++;
				map_path_version.put(path, new_version);
				return(new_version);
			}
			else {
				map_path_version.put(path,1);
				return(1);
			}
    	}
    }
    /**
     * read on server
     * @param path is the original path of a file
     * @param start_offset is the offset that server should write start with
     * @param size the max size of byte
     * @return byte[] , the byte array carrier to store the read result
     */
    public byte[] read(String path, long start_offset, long size) throws RemoteException {
    	synchronized (cacheLock) {
	        String server_path = getServerPath(path);
	        System.err.println("in server read, try to read: "+server_path);
	        byte[] b = new byte[(int)size];
	        RandomAccessFile raf = null;
	        try {
	            // locate to the position, and read a chunk
	            raf = new RandomAccessFile(server_path, "r");
	            raf.seek(start_offset);
	            raf.read(b);
	            raf.close();       
	            return b;
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	        return b;
    	}
    }
    /**
     * write on server
     * @param path is the original path of a file
     * @param start_offset is the offset that server should write start with
     * @param b is the content byte array to write on server
     * @return void
     */

    public synchronized void write(String path, long start_offset, byte[] b) throws RemoteException {
    	int version;
        String server_path = getServerPath(path);
        System.err.println("in server write, try to write: "+server_path);      
        
        try {
            RandomAccessFile raf = new RandomAccessFile(server_path, "rw");
            // locate to the position, and write
            raf.seek(start_offset);
            raf.write(b);
           // str= new String (b);
            //System.err.println("in server write, write:  "+str);
            raf.close();
        }
        catch (Exception e){
            System.err.println("Error while writing to server in chunks");
        }
    }
    
    /**
     * delete the file from server, this method will be used by a proxy's unlink or deleting a file before writing on server
     * @param path ,the orig_path of the file
     * @return true if succeed, or false if fail
     */
    
    public synchronized boolean deleteSeverFile(String path) throws RemoteException{
    		synchronized (cacheLock){
	            System.err.println("Server:deleteSeverFile");
	            //update map_path_version
	            if (map_path_version.containsKey(path)){
	            	map_path_version.remove(path);
	            }
	            // delete the file
	            String server_path = getServerPath(path);
	            File f = null;
	            try {
	                f = new File(server_path);
	                if (f.exists()) {
	                    f.delete();
	                    return true;
	                } 
	                else {
	                    System.err.println("Error:Sever delete old file. file doesn't exist at all");
	                }
	            }
	            catch (Exception e) {
	                e.printStackTrace();
	            }
	            return false;
    		}
    }
    
    public static void main(String[] args) throws IOException{
    	
        Server.listenport = Integer.parseInt(args[0]);
        Server.fileroot = args[1];
        
        try {
            LocateRegistry.createRegistry(Server.listenport);
        }
        catch (RemoteException e) {
            System.err.println("Failed to create RMI registry");
        }
		
        Server server = null;
        try {
            server = new Server(fileroot);
        } 
        catch (RemoteException e) {
            System.err.println("Failed to create server " + e);
            System.exit(1);
        }
        try {
            Naming.rebind(String.format("//127.0.0.1:%d/Server", listenport), server);
        } 
        catch (Exception e) {
            System.err.println("Error in server Naming.rebind"); //you probably want to do some decent logging here
        } 	
        System.err.println("Server fileroot: "+fileroot);
        System.err.println("Server is ready");
    }
}
