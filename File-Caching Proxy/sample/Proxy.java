/* Sample skeleton for proxy */

import java.io.*;
import java.util.*;
import java.util.Queue;
import java.nio.file.OpenOption;

public class Proxy {
	
	public static Stack <Integer> fd_available = new Stack<Integer>();
	public static Set <Integer> fd_used = Collections.synchronizedSet(new HashSet<Integer>());	
	public static Map<Integer, String> map_fd_mode = Collections.synchronizedMap(new HashMap<Integer, String>());
	public static Map<Integer, File> map_fd_file = Collections.synchronizedMap(new HashMap<Integer, File>());
	public static Map<Integer, RandomAccessFile> map_fd_raf = Collections.synchronizedMap(new HashMap<Integer, RandomAccessFile>());
	
	private static class FileHandler implements FileHandling {
		
		private ArrayList<Integer> fd_array = new ArrayList<Integer>();
		//static Stack <Integer> fd_available = new Stack<Integer>();
		
		public int open( String path, OpenOption o ) {
			
			String mode = null;
			String symbol = null;
			
			System.out.println("path:"+ path);
			System.out.println("OpenOption:"+ o);
			
			if(o.toString() == "READ") {
				mode = "r";
				
				System.out.println("OpenOption:"+ "READ");
			}
			else if(o.toString() == "WRITE"){
				mode = "rw";
				System.out.println("OpenOption:"+ "WRITE");
			}
			else if(o.toString() == "CREATE"){
				mode = "rw";
				System.out.println("OpenOption:"+ "CREATE");
			}
			else if(o.toString() == "CREATE_NEW"){
				mode = "rw";
				System.out.println("OpenOption:"+ "CREATE_NEW");
			}
			else{
				return Errors.EINVAL;
			}
			
			symbol = o.toString();
			
			File file = new File(path);
			
			if(!file.exists() && symbol != "CREATE"  && symbol != "CREATE_NEW"){
				System.out.println("Error: open a file pathname not exist");
				return Errors.ENOENT;
			}
			if(file.exists() && symbol == "CREATE_NEW"){
				System.out.println("Error:create a file pathname already existed");
				return Errors.EEXIST;
			}
			if(file.isDirectory() && mode !="r"){
				System.out.println("Error: try to open with a directory but not mode 'r'");
				return Errors.EISDIR;
			}
			
			/*get here mean that open a file successfully*/
			
			int fd = fd_available.pop();
			fd_used.add(fd);
			try {
				System.out.println("raf open mode: " + mode);
				RandomAccessFile raf = new RandomAccessFile(path, mode);	
				map_fd_raf.put(fd, raf);
			} catch (Exception e) {
				System.out.println("Error: An error occurred while creating RAF.");
			}
			map_fd_file.put(fd, file);
			map_fd_mode.put(fd, mode);
			
			
			System.out.println("open fd: " + Integer.toString(fd));
			return fd;			
		}
		
		public int close( int fd ){
			System.out.println("This is close.");
			System.out.println("close fd: " + Integer.toString(fd));
			
			if(!fd_used.contains(fd)){
				System.out.println("Read Error: Bad file descriptor");
				return Errors.EBADF;
			}
			
			String mode = map_fd_mode.get(fd);
			File file = map_fd_file.get(fd);
			RandomAccessFile raf = map_fd_raf.get(fd);
						
			fd_available.push(fd);
			fd_used.remove(fd);
			map_fd_mode.remove(fd);
			map_fd_file.remove(fd);
			map_fd_raf.remove(fd);			
			try {
				raf.close();
			}
			catch(Exception e) {
				System.out.println("Error: An error occurred while closing raf.");
			}
			return 0;
		}

		public long write( int fd, byte[] buf ) {
			System.out.println("This is write.");
			System.out.println("write fd: " + Integer.toString(fd));
			if(!fd_used.contains(fd)){
				System.out.println("Write Error: Bad file descriptor");
				return Errors.EBADF;
			}
			String mode = map_fd_mode.get(fd);
			File file = map_fd_file.get(fd);
			
			if(mode != "rw") {
				return Errors.EBADF;
			}
			if(file.isDirectory()){
				System.out.println("Error: Try to write to a directory");
				return Errors.EISDIR;
			}
			RandomAccessFile raf = map_fd_raf.get(fd);
			int bytes = 0;
			try {
				raf.write(buf);
				System.out.println("write file length:" + Integer.toString(buf.length));
				bytes = buf.length;
			} 
			catch (Exception e) {
				System.out.println("Error: An error occurred while writing file.");
			}
			return(bytes);
		}

		public long read(int fd, byte[] buf ) {
			
			System.out.println("This is read.");
			System.out.println("read fd: " + Integer.toString(fd));
			
			if(!fd_used.contains(fd)){
				System.out.println("Read Error: Bad file descriptor");
				return Errors.EBADF;
			}
			
			/*
			here is a valid fd for read
			*/
			
			String mode = map_fd_mode.get(fd);
			File file = map_fd_file.get(fd);
			
			if(file.isDirectory()){
				System.out.println("Error: Try to read a directory");
				return Errors.EISDIR;
			}
			RandomAccessFile raf = map_fd_raf.get(fd);
			
			int bytes = 0;
			try {
				bytes = raf.read(buf);
				if(bytes == -1){
					System.out.println("end of file, return 0.");
					return 0;
				}
			} 
			catch (Exception e) {
				System.out.println("Error: An error occurred while reading file.");
			}
			return bytes;
		}

		public long lseek( int fd, long pos, LseekOption o ) {
			System.out.println("This is lseek.");
			System.out.println("lseek fd: " + Integer.toString(fd));
			if(!fd_used.contains(fd)){
				System.out.println("Error: Bad file descriptor");
				return Errors.EBADF;
			}
			String mode = map_fd_mode.get(fd);
			File file = map_fd_file.get(fd);

			if(file.isDirectory()){
				System.out.println("Error: Try to write to a directory");
				return Errors.EISDIR;
			}
			
			RandomAccessFile raf = map_fd_raf.get(fd);
			/*
			long file_size = raf.length();
			long cur_offset = raf.getFilePointer();
			long offset = 0;
			*/
			long file_size = 0;
			long cur_offset = 0;
			try {
				file_size = raf.length();
				cur_offset = raf.getFilePointer();
			}
			catch(Exception e){
				System.out.println("Error: An error occurred during lseek: af.length() or raf.getFilePointer() .");
			}
			
			long offset = 0;
			System.out.println(o.toString());
			
			if(o.toString() == "FROM_START") {
				offset = pos;
				System.out.println("FROM_START");
			}
			else if(o.toString() == "FROM_END"){
				offset = file_size + pos;
				System.out.println("FROM_END");
			}
			else if(o.toString() == "FROM_CURRENT"){
				offset = cur_offset + pos;
				System.out.println("FROM_CURRENT");
			}
			else{
				return Errors.EINVAL;
			}
			
			try {
				raf.seek(offset);
			} 
			catch (Exception e) {
				System.out.println("Error: An error occurred during lseek.");
			}
			
			return offset;
		}

		public int unlink(String path) {
			System.out.println("This is unlink.");
			System.out.println("unlink path: " + path);
			
			File file = new File(path);
			if(!file.exists()){
				System.out.println("Error: Try to unlink a file that doesn't exist");
				return Errors.ENOENT;
			}
			if(file.isDirectory()){
				System.out.println("Error: Try to unlink a directory");
				return Errors.EISDIR;
			}
			file.delete();
			return 0;
		}
		public void clientdone() {
			return;
		}
	}
	private static class FileHandlingFactory implements FileHandlingMaking {
		public FileHandling newclient() {
			return new FileHandler();
		}
	}

	public static void main(String[] args) throws IOException {
		System.out.println("Hello World");
		/*init a fd_available stack to store fd(3-1024)*/
		for(int i = 1024;i>=3;i = i - 1) {
			fd_available.push(i);			
		}
		(new RPCreceiver(new FileHandlingFactory())).run();
	}
}

