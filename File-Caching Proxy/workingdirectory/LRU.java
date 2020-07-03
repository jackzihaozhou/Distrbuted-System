/**
 * LRU, a LRUcache for RPC caching with automatically deleting files method
 * @author Zihao ZHOU
 * @andrewID zihaozho
 */
import java.io.File;
import java.util.*;

import java.util.*;

class LRU{
    public static Map<String, Integer> map;
    public final int capacity;
    public final int max_entry = 5000;
    public static int cursize=0;
    public LRU(int capacity){
        this.capacity = capacity;
        map = new LinkedHashMap<String, Integer>(max_entry, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry eldest){
                return cursize > capacity;  
            }
        };
    }
    //get current size of LRU
    public synchronized int getLRUCursize() {
    	return(this.cursize);
    }
    //force evicting in case current size exceed the capacity
    public synchronized void forceEvict(String avoid_path) {
    	String evict_path = selectevict(this.cursize);
    	System.err.println("avoid evict : "+ avoid_path);
    	System.err.println("choose evict : "+ evict_path);
    	if(evict_path.equals(avoid_path) || evict_path == "null") {
    		System.err.println("avoid evict this "+ avoid_path);
    		return;
    	}
    	cursize = cursize - map.get(evict_path);
    	deleteOldCache(evict_path);
    	
    	System.err.println("in LRU, force delete one cache");
    }
    //select a cache to evict by LRU rule
    public synchronized String selectevict(int current_size) {
    	assert !map.isEmpty();
    	//System.err.println("in LRU, delete old cache "+ file.toString() + " successfully: ");
    	Object[] a = map.keySet().toArray();
    	int i=0;
    	boolean flag = false;
    	for(;i<a.length;i++) {
    		if (    Proxy.map_path_readcount.get(a[i].toString())<=0 && (current_size-map.get(a[i].toString()) <= capacity)   ) {
    			flag = true;
    			break;
    		}
    	}
    	if(flag == true) {
    		String lastevict = a[i].toString();
    		return(lastevict);
    	}
    	else {
    		return("null");
    	}	    	

    }
    //delete this file after evicting from LRUcache
    public synchronized void deleteOldCache(String path) {
    	String path_master = Proxy.map_path_master.get(path);
    	File file = new File(path_master);	
    	if(file.delete()) {
    		System.err.println("in LRU, delete old cache "+ file.toString() + " successfully: ");
    	}
    	assert  Proxy.map_path_readcount.get(path)<=0;
    	Proxy.map_path_master.remove(path);
    	Proxy.map_path_version.remove(path);
    	map.remove(path);
    }
    //put a new cache pair into LRU, if exceed the capacity, automatically evict an old cache
    public synchronized boolean put(String key, Integer value) {
    	String evict_path;
    	int prev_cache_size = 0;
    	boolean same_cache = false;
    	if(!map.containsKey(key)) {
    		cursize = cursize + value;
    		System.err.println(key+" not in LRU cache");
    	}
    	else {
    		if(map.get(key) != value) {
    			System.err.println(key+" is changed size in LRUcache, just change time order and its size");
    			prev_cache_size = map.get(key);
    			cursize = cursize - prev_cache_size + value;//total size after change
    			same_cache = true;
    		}
    		else {
    			System.err.println(key+" is already in LRUcache, just change time order");
    		}
    		
    	}
    	if(cursize > capacity) {
    		evict_path = selectevict(cursize);
    		if (evict_path != "null") {
    			cursize = cursize - map.get(evict_path);
    			deleteOldCache(evict_path);
    			System.err.println("current cachesize is "+cursize);
    			map.put(key, value);    
    			return(true);
    		}
    		else {
    			//all caches in LRU are busy, so don't cache this item
    			
    			System.err.println("all caches in LRU are busy, so don't cache this item");
    			deleteOldCache(key);
    			if(same_cache == false) {
    				cursize = cursize - value;
    			}
    			else {
    				cursize = cursize - value + prev_cache_size;
    			}
    			return(false);
    			
    		}
    	}
    	else {
    		map.put(key, value); 
    		return(true);
    	}
    }
    //remove a cache rudely
    public synchronized boolean remove(String key) {
    	if(map.containsKey(key)) {
    		map.remove(key);
    		return true;
    	}
    	else {
    		return false;
    	}
    }
}