import java.rmi.*;

public interface Iserver extends Remote {
    /**
     * proxy use this to touch the file at server, to get the version, length and state(whether exist) 
     * @param path is the original path of a file
     * @return int[3], the aggregation of version, length and state corresponding to the original path, version :
     * version = version number
     * filelen = if existed, the total byte of this file 
     * state 0: file not exist, 1: file exist
     */
	public int[] touchFile(String path) throws RemoteException;
	
	
    /**
     * update the version map at server
     * @param path is the original path of a file
     * @return the version number of the after updating correspoding to the given the original path
     */
	public int updateVersion(String path) throws RemoteException;
    /**
     * read on server
     * @param path is the original path of a file
     * @param start_offset is the offset that server should write start with
     * @param size the max size of byte
     * @return byte[] , the byte array carrier to store the read result
     */
    public byte[] read(String path, long start_offset, long size) throws RemoteException;
    
    
    /**
     * write on server
     * @param path is the original path of a file
     * @param start_offset is the offset that server should write start with
     * @param b is the content byte array to write on server
     * @return void
     */
    public void write(String path, long start_offset, byte[] b) throws RemoteException;
    /**
     * delete the file from server, this method will be used by a proxy's unlink or deleting a file before writing on server
     * @param path ,the orig_path of the file
     * @return true if succeed, or false if fail
     */
    public boolean deleteSeverFile(String path) throws RemoteException;

}