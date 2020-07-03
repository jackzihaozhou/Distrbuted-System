/**
 * Proxy, a simple proxy for RPC caching with given cahce folder and cache limit size
 * @author Zihao ZHOU
 * @andrewID zihaozho
 */

import java.io.*;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.sql.Timestamp;

public class Proxy {
	//a set used to check fd whether is available
	public static Stack <Integer> fd_available = new Stack<Integer>();
	//a stack use to allocate fd
	public static Set <Integer> fd_used = Collections.synchronizedSet(new HashSet<Integer>());	
	
	//the following map is mapping fd to open_mode, file , raf and operating path
	public static Map<Integer, String> map_fd_mode = Collections.synchronizedMap(new HashMap<Integer, String>());
	public static Map<Integer, File> map_fd_file = Collections.synchronizedMap(new HashMap<Integer, File>());
	public static Map<Integer, RandomAccessFile> map_fd_raf = Collections.synchronizedMap(new HashMap<Integer, RandomAccessFile>());
	public static Map<Integer, String> map_fd_path = Collections.synchronizedMap(new HashMap<Integer, String>());
	
	//Mapping from path to its version
	public static Map<String, Integer> map_path_version = Collections.synchronizedMap(new HashMap<String, Integer>());//!!
	
	//Mapping from path to mastercache
	public static Map<String, String> map_path_master = Collections.synchronizedMap(new HashMap<String, String>());
	//Mapping from path to readcount
	public static Map<String, Integer> map_path_readcount = Collections.synchronizedMap(new HashMap<String, Integer>());
	//Mapping from operating path to path
	public static Map<String, String> map_operate_path = Collections.synchronizedMap(new HashMap<String, String>());
	


	
	
	public static String ip;
	public static int port;
	public static String cachefolder;
	public static int cachesize;	
	public static int CHUNKSIZE = 128000;
	public static LRU LRUcache;
	
	static Iserver server = null;
	//a static lock for concurrency
	static Object cacheLock = new Object();
	
	private static class FileHandler implements FileHandling{
				    
        public FileHandler() {
        	//this constructor connect to lanuached server automatically
	        String url = String.format("//%s:%d/Server", ip, port);
	        try {
	        	server =  (Iserver) Naming.lookup(url);
	        }
	        catch (Exception e) {
	            System.exit(1); 
	        }

            if (server == null) {
            	System.exit(1); 
            }
            else {
            	System.err.println("Connect successfully");
            }
        }
        /**
         * add ReadCount if a reader want to read cache mastercopy correspoding to this path
         * @param path the original path of a file
         * @return void
         */
        public synchronized void addReadCount(String path){
        	
        	assert map_path_readcount.containsKey(path);
        	
        	int value = map_path_readcount.get(path);
        	value++;
        	map_path_readcount.put(path, value);
        }
        /**
         * subtract ReadCount if a reader finish reading cache mastercopy correspoding to this path
         * @param path the original path of a file
         * @return void
         */
        public synchronized void minusReadCount(String path){
        	
        	assert map_path_readcount.containsKey(path);
        	
        	int value = map_path_readcount.get(path);
        	value--;
        	map_path_readcount.put(path, value);
        }
        public synchronized int checkReadCount(String path){
        	
        	assert map_path_readcount.containsKey(path);

    		return(map_path_readcount.get(path));

        }
        /**
         * attach prefix in front of the given path 
         * @param path the original path of a file
         * @return String Cahcepath, with cache folder prefix in front of the path 
         */
        private synchronized String getCachePath(String path) {
            StringBuilder sb = new StringBuilder(path);
            if (cachefolder.charAt(cachefolder.length()-1) != '/') {
                sb.insert(0, "/");
            }
        	sb.insert(0, cachefolder);

            return sb.toString();
        }
        
        
        /**
         * get proxy side version of given path
         * @param path the original path of a file
         * @return version if have, or -1 if no version of this path
         */
        public synchronized int getVersion(String path) throws RemoteException{
        	if(map_path_version.containsKey(path)) {
        		return(map_path_version.get(path));
        	}
        	else {
        		return(-1);
        	}
        }
        /**
         * download file from server
         * @param path the original path of a file
         * @param store_path the absolute path to download the file
         * @param fsize the actual size of the file you want to download
         * @param server_version of this file
         * @return void
         */
        public synchronized void getFileFromServer(String path, String store_path,int fsize,int server_version){
            try {

            	assert fsize>0;
            	if(map_path_version.containsKey(path)) {
                	if(map_path_version.get(path)==server_version) {
                		return;
                	}
            	}
                RandomAccessFile raf = new RandomAccessFile(store_path, "rw");
                long cnt = 0;
                while (cnt < fsize) {
                    long bytearraysize = Math.min(CHUNKSIZE, fsize - cnt);
                    byte[] b = server.read(path, cnt, bytearraysize);
                    raf.write(b);
                    cnt += b.length;
                }
                raf.close();
                //update map_path_version
                map_path_version.put(path,server_version);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        /**
         * create folder according to input folderpath
         * @param folderpath ,the path to create folder
         * @return void
         */
        public synchronized void createFolder(String folderpath) {
        	File file = new File(cachefolder + "/"+folderpath);
            if (!file.exists()) {
                if (file.mkdir()) {
                } else {
                }
            }
        }
        /**
         * simplify a complex path by dealing with '..' or '.'
         * @param path ,the complex original path
         * @return String path, the simplified path
         */
        public synchronized String simplifyPath(String path){
            if (path == null) return "/";
            String[] tokens = path.split("/");
            // use a stack to eliminate .. and .
            java.util.LinkedList<String> stk = new java.util.LinkedList<String>();
            for (int i = 0; i < tokens.length; ++i) {
                if (tokens[i].length() == 0 || tokens[i].equals(".")) {
                    continue;
                }
                else if (tokens[i].equals("..")) {

                    if (!stk.isEmpty()){
                        stk.removeLast();
                    }
                    else {
                    	stk.add(tokens[i]);
                    }
                }
                else {
                    stk.add(tokens[i]);
                }
            }
            if (stk.isEmpty()){
                return "/";
            }
            StringBuilder s = new StringBuilder();
            while (!stk.isEmpty()) {
                s.append("/");
                s.append(stk.remove());
            }
            return s.toString().substring(1, s.toString().length());
        }
        /**
         * check a path whether it is out of cache holder boundary
         * @param path ,the complex original path
         * @return String path, the simplified path
         */
        public synchronized boolean checkPath(String path){
        	String[] tokens = path.split("/");
        	if(tokens[0].equals("..")){
        		return false;
        	}
        	else{
        		return true;
        	}
        }
        /**
         * allocate a unique path for original path by sticking a Timestamp
         * @param path ,the original path of the file
         * @return String, the unique path with sticking a Timestamp
         */
        public synchronized String allocateUniqueProxyPath(String path){
            // get timestamp
        	String proxy_path = getCachePath(path);
            java.util.Date date= new java.util.Date();
            Timestamp ts = new Timestamp(date.getTime());
            String tsstring = ts.toString().replaceAll(" ", "_");
            return proxy_path + "_at_" + tsstring;
        }
        /**
         * copy a file from 'src_proxy_path' to 'des_proxy_path'
         * @param src_proxy_path ,source path
         * @param des_proxy_path ,destination path
         * @return true if success, or false if fail
         */
        public synchronized boolean copyFile(String src_proxy_path,String des_proxy_path){
            
            try {
                File src = new File(src_proxy_path);
                assert src.exists();
                
                File dst = new File(des_proxy_path);
                Files.copy(src.toPath(), dst.toPath());
            } 
            catch (Exception e) {
                e.printStackTrace();
                return false;
            }
            return true;
        }
        /**
         * delete the mastercopy of the path file at proxy if there is no occupation by read
         * @param path ,the original path of the file
         * @return void
         */
        public synchronized void suspendDeleteOldCache(String path) {
        	String path_master = map_path_master.get(path);
        	if(map_path_readcount.containsKey(path)) {
        		if(map_path_readcount.get(path)>0) {
        			return;
        		}
        	}
        	
        	File file = new File(path_master);	
        	if(file.delete()) {
        	}
        }
        
		public synchronized int open( String path, OpenOption o){
            /******************************************************************
             check whether exist fileDescription in both server and proxy, if existed, check the version of them
             *****************************************************************/			 
			path = simplifyPath(path);
			if(!checkPath(path)) {
				return(Errors.EPERM);
			}
			
			
            if (path.contains("/")) {
                // find the last position of '/'
                int pos = 0;
                for (int i = 0; i < path.length(); i++){
                    if (path.charAt(i) == '/') {
                        pos = i;
                    }
                }
                String subdirpath = path.substring(0, pos);
                createFolder(subdirpath);
            }
            synchronized (cacheLock) {
				String master_cachepath = null,operate_cachepath = null;
	        	if(Proxy.map_path_master.containsKey(path)) {
	        		master_cachepath = map_path_master.get(path);
	        	}
	        	
	        	boolean bool_flag = false;
	
				try {
		        	int[] touch = new int[2];
		        	int server_version = 0;
		        	int fileLen = 0;	
		        	int server_state = 0;	
		        	touch = server.touchFile(path);
		        	server_version = touch[0];
		        	fileLen = touch[1];	
		        	server_state = touch[2];	
	
		            if(o.toString() == "READ"){
		            	if(getVersion(path) == server_version && server_state == 1) {
	
		            		//add read count before reading
		            		if(!map_path_readcount.containsKey(path)) {
		            			map_path_readcount.put(path, 1);
		            		}
		            		else {
		            			addReadCount(path);
		            		}
		            		assert master_cachepath != null;
		            		operate_cachepath = master_cachepath;
		            		
	            		
		            	}
		            	else if (server_state == 1){
		            		//no cache or cache not valid, and server has this file

	            			if(map_path_version.containsKey(path)) {
	            				//cache existed but invalid	
		            			suspendDeleteOldCache(path);		    				
		    					LRUcache.remove(path);
	            			}		            		
		            		operate_cachepath = allocateUniqueProxyPath(path);
		            		
	            			getFileFromServer(path,operate_cachepath,fileLen,server_version);
	            			//after fetch it from server, put this file as master cachecopy
		            		Proxy.map_path_master.put(path,operate_cachepath);
		            		
		            		//add read count before reading
		            		if(!map_path_readcount.containsKey(path)) {
		            			map_path_readcount.put(path, 1);
		            		}
		            		else {
		            			addReadCount(path);
		            		}
		            		
		            	}
		            	else {
		            		//try to read a unexiusted file return error
		            		return Errors.ENOENT;
		            	}
		            }
		            else {//WRITE
		            	if(getVersion(path) == server_version && server_state == 1) {
		            		
		            		assert master_cachepath != null;
		            		operate_cachepath = allocateUniqueProxyPath(path);
		            		bool_flag = copyFile(master_cachepath,operate_cachepath);
		            		
		            		File temp_file = new File(map_path_master.get(path));
		            		if(!LRUcache.map.isEmpty()) {
		            			//force to evict an item from cache because it the total size will exceed limit if create a write copy
			            		if( (LRUcache.getLRUCursize()+(int)(temp_file.length())) > LRUcache.getLRUCursize() ) {
			            			LRUcache.forceEvict(path);            			
			            		}
		            		}
			                
		            	}
		            	else if (server_state == 1){
		            		//cache invalid or unexisted but server has this file
	            			if(map_path_version.containsKey(path)) {
	            				//cache existed but invalid	
		            			suspendDeleteOldCache(path);		    				
		    					LRUcache.remove(path);
	            			}
		            		String temp_operate_cachepath = allocateUniqueProxyPath(path);
		            		getFileFromServer(path,temp_operate_cachepath,fileLen,server_version);
		            		Proxy.map_path_master.put(path,temp_operate_cachepath);	      
		            		
		            		File temp_file = new File(map_path_master.get(path));
		            		if(!LRUcache.map.isEmpty()) {
		            			//force to evict an item from cache because it the total size will exceed limit if create a write copy
			            		if( (LRUcache.getLRUCursize()+(int)(temp_file.length())) > LRUcache.getLRUCursize() ) {
			            			LRUcache.forceEvict(path);            			
			            		}
		            		}
		            		
		            		operate_cachepath = allocateUniqueProxyPath(path);
		            		bool_flag = copyFile(temp_operate_cachepath,operate_cachepath);
		            		//set new write copy with 0 readcount
	            			map_path_readcount.put(path, 0);
	            			
		            	}
		            	else{
		            		//write a new file
		            		operate_cachepath = allocateUniqueProxyPath(path);		
		            		//set new write copy with 0 readcount
		            		map_path_readcount.put(path, 0);
		            	}
		            }
				}
				catch (Exception e) {
	                e.printStackTrace();
	            }
	
				
				map_operate_path.put(operate_cachepath, path);
			
            /******************************************************************
              now, the operate_cachepath should be the file that is actually used
             *****************************************************************/
			
            
				String mode = null;
				String symbol = null;
				
		
				if(o.toString() == "READ"){
					mode = "r";
				}
				else if(o.toString() == "WRITE"){
					mode = "rw";
				}
				else if(o.toString() == "CREATE"){
					mode = "rw";
				}
				else if(o.toString() == "CREATE_NEW"){
					mode = "rw";
				}
				else{
					return Errors.EINVAL;
				}
				
				symbol = o.toString();
				File file = new File(operate_cachepath);
				
				
				
				if(!file.exists() && symbol != "CREATE"  && symbol != "CREATE_NEW"){
					return Errors.ENOENT;
				}
				if(file.exists() && symbol == "CREATE_NEW"){
					return Errors.EEXIST;
				}
				if(file.isDirectory() && mode !="r"){
					return Errors.EISDIR;
				}
				
				/*get here mean that open a file successfully*/
				
				int fd = fd_available.pop();
				fd_used.add(fd);
				try {
					RandomAccessFile raf = new RandomAccessFile(operate_cachepath, mode);	
					map_fd_raf.put(fd, raf);
				} catch (Exception e) {
				}
				map_fd_file.put(fd, file);
				map_fd_mode.put(fd, mode);
				map_fd_path.put(fd,operate_cachepath);	
				return fd;
            }		
		}
		
		public synchronized int close( int fd ){
			synchronized (cacheLock) {
				if(!fd_used.contains(fd)){
					return Errors.EBADF;
				}		
				
				String mode = map_fd_mode.get(fd);
				File file = map_fd_file.get(fd);
				RandomAccessFile raf = map_fd_raf.get(fd);
				String operate_cachepath = map_fd_path.get(fd);
				String path = map_operate_path.get(operate_cachepath);
				if(mode =="rw"){				
		            try {
		            	//after finishing writing, push it to server as latest version
		                long filesize = raf.length();
		                long cnt = 0;
		                server.deleteSeverFile(path);
		                raf.seek(0);
		                while (cnt < filesize) {
		                    long bytearraysize = Math.min(CHUNKSIZE, filesize - cnt);
		                    byte[] b = new byte[(int)bytearraysize];
		                    raf.read(b);
		                    server.write(path, cnt, b);
		                    cnt += bytearraysize;
		                }
		                raf.close();
		                //after write a new copy of original master cachecopy, make this as new master cachecopy and try deleting old cache
		                if(map_path_readcount.containsKey(path)) {	                
							if(map_path_readcount.get(path)<=0 && map_path_master.get(path) != operate_cachepath) {
								//if there are reader reading on this obselete copy, don't delete and let the last reader delete it
								File prev_master_cachefile = new File(map_path_master.get(path));
					        	if(prev_master_cachefile.delete()){
					            	Proxy.map_path_master.remove(path);
					            	Proxy.map_path_version.remove(path);
					        	}
							}
		                }
		                //after fetch it from server, put this file as master cachecopy
		                map_path_master.put(path, operate_cachepath);
			            try {	            	
			            	//update cache version
			            	int new_version = server.updateVersion(path);
			            	map_path_version.put(path, new_version);
	
			            }
			            catch (Exception e) {
			            	e.printStackTrace();
			            }
			                       		
		                map_fd_raf.remove(fd);
		            } 
		            catch (Exception e) {
		                e.printStackTrace();
		            }
				}
	            else {
	            	//(mode =="r")	
					try {
						raf.close();
						map_fd_raf.remove(fd);
						minusReadCount(path);
						// if the reading cache is nolonger a master copy, it means it is overwritten by a writer, so after reading it, delete it
						if(map_path_readcount.get(path)<=0 && map_path_master.get(path) != operate_cachepath) {
				        	if(file.delete()){
				            	Proxy.map_path_master.remove(path);
				            	Proxy.map_path_version.remove(path);
				        	}
						}
					}
					catch(Exception e) {
						System.err.println("Error: An error occurred while closing raf.");
					}
		            
	            }
				synchronized (cacheLock) {
					// put this master copy it to LRUcache
					LRUcache.put(path, (int)(file.length()));  
				}
				
				fd_available.push(fd);
				fd_used.remove(fd);
				map_fd_mode.remove(fd);
				map_fd_file.remove(fd);							
				map_fd_path.remove(fd);		
            
				return 0;
			}
		}
		
		public synchronized long write( int fd, byte[] buf ) {
			if(!fd_used.contains(fd)){
				return Errors.EBADF;
			}
			String mode = map_fd_mode.get(fd);
			File file = map_fd_file.get(fd);
			
			if(mode != "rw") {
				return Errors.EBADF;
			}
			if(file.isDirectory()){
				return Errors.EISDIR;
			}
			RandomAccessFile raf = map_fd_raf.get(fd);
			int bytes = 0;
			try {
				raf.write(buf);
				bytes = buf.length;
			} 
			catch (Exception e) {
				System.err.println("Error: An error occurred while writing file.");
			}
			return(bytes);
		}
		
		public synchronized long read(int fd, byte[] buf ) {
			
			
			if(!fd_used.contains(fd)){
				return Errors.EBADF;
			}	

			String mode = map_fd_mode.get(fd);
			File file = map_fd_file.get(fd);
			
			if(file.isDirectory()){
				return Errors.EISDIR;
			}
			RandomAccessFile raf = map_fd_raf.get(fd);
			
			int bytes = 0;
			try {
				bytes = raf.read(buf);
				if(bytes == -1){
					return 0;
				}
			} 
			catch (Exception e) {
				System.err.println("Error: An error occurred while reading file.");
			}
			return bytes;
		}

		public synchronized long lseek( int fd, long pos, LseekOption o ){
			if(!fd_used.contains(fd)){
				return Errors.EBADF;
			}
			String mode = map_fd_mode.get(fd);
			File file = map_fd_file.get(fd);

			if(file.isDirectory()){
				return Errors.EISDIR;
			}
			
			RandomAccessFile raf = map_fd_raf.get(fd);

			long file_size = 0;
			long cur_offset = 0;
			try {
				file_size = raf.length();
				cur_offset = raf.getFilePointer();
			}
			catch(Exception e){
			}
			
			long offset = 0;
			
			if(o.toString() == "FROM_START") {
				offset = pos;
			}
			else if(o.toString() == "FROM_END"){
				offset = file_size + pos;
			}
			else if(o.toString() == "FROM_CURRENT"){
				offset = cur_offset + pos;
			}
			else{
				return Errors.EINVAL;
			}
			
			try {
				raf.seek(offset);
			} 
			catch (Exception e) {
			}
			
			return offset;
		}

		public synchronized int unlink(String path) {
			
			synchronized (cacheLock) {
	            
	            if(map_path_master.containsKey(path)) {
	            	map_path_master.remove(path);
	            	map_path_version.remove(path);
	            	if(map_path_readcount.containsKey(path)) {
	            		map_path_readcount.put(path,0);
	            	}
	            }
	                       
	            try {
	            	//server delete file and its correspoding Filedescription
	                if (server.deleteSeverFile(path)) {//fail
	                	return 0;                    
	                } 
	                else {
	                	return Errors.ENOENT;
	                }
	            } 
	            catch (RemoteException e) {
	                e.printStackTrace();
	            }
	            return 0;
			}
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
		try {
			ip = args[0];
			port = Integer.parseInt(args[1]);
			cachefolder = args[2];
			cachesize = Integer.parseInt(args[3]);
		}
		catch(Exception e) {
			System.err.println("Error: An error in input args");
		}
		Proxy.LRUcache = new LRU(cachesize);
		/*init a fd_available stack to store fd(3-1024)*/
		for(int i = 1024;i>=3;i = i - 1) {
			fd_available.push(i);			
		}
		(new RPCreceiver(new FileHandlingFactory())).run();
	}
}

