import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.*;
public class Server extends UnicastRemoteObject implements Iserver{
    
    private static String cloud_ip;
    private static int cloud_port;    
    private static int vm_id;    
    private static Iserver master_vm;
    private static ServerLib SL;
    private static long interval;    
    private static boolean machine_live_status = true;
    private static List<Integer> Frontend_machine_List;
    private static List<Integer> Apptier_machine_List;
    private static long last_scale_timestamp;
    private static final long Scale_threshold = 5000;    
    private static AtomicInteger get_request_num;    
    private static ConcurrentLinkedQueue<Cloud.FrontEndOps.Request> AppQueue;    
    private static ConcurrentHashMap<String, String> cache;
    private static Cloud.DatabaseOps Icache = null;
    private static Cloud.DatabaseOps Database = null;
   
    static Object Lock = new Object();
    public Server() throws RemoteException {
    }
    
    public static void main ( String args[] ) throws Exception {
    	
        if (args.length != 3) throw new Exception("Need 3 args: <cloud_ip> <cloud_port> <VM id>");       
        cloud_ip = args[0];
        cloud_port = Integer.parseInt(args[1]);
        vm_id = Integer.parseInt(args[2]);
        SL = new ServerLib( args[0], Integer.parseInt(args[1]));      
        Server this_server = new Server();
        LocateRegistry.getRegistry(cloud_ip, cloud_port).bind("//127.0.0.1/no" + String.valueOf(vm_id), this_server); //Registry
        
        if(vm_id == 1) {
            //frontend main frontend server
        	Database = SL.getDB();
        	cache = new ConcurrentHashMap<>(1024);
            Frontend_machine_List = Collections.synchronizedList(new ArrayList<Integer>());
            Apptier_machine_List = Collections.synchronizedList(new ArrayList<Integer>());
            AppQueue = new ConcurrentLinkedQueue<Cloud.FrontEndOps.Request>();
            
            setAppNum(1);         
            while(Apptier_machine_List.size() == 0){
            }
            SL.register_frontend();
            Cloud.FrontEndOps.Request r = null;
            //get two next requeststo determine initial client interval
            while ((r = SL.getNextRequest()) == null){
            }
            AppQueue.add(r);
            long time1 = System.currentTimeMillis();            
            while ((r = SL.getNextRequest()) == null){
            }
            AppQueue.add(r);            
            long time2 = System.currentTimeMillis();
            interval = time2 - time1;
            initialization(interval);
            
            //before one apptier server is "running", frontend machine do both frontend and apptier jobs 
            while(Apptier_machine_List.size()==0 || SL.getStatusVM(Apptier_machine_List.get(0)) != Cloud.CloudOps.VMStatus.Running) {
            	if(AppQueue.size()>0){
            		r = AppQueue.poll();
            		SL.processRequest(r,this_server);
            	}
            	if(AppQueue.size()>0){
            		r = AppQueue.poll();
            		SL.processRequest(r,this_server);
            	}
            	dropFrontImpossible();
                while ((r = SL.getNextRequest()) == null) {
                } 
                AppQueue.add(r);
            }
            get_request_num = new AtomicInteger(0);
            int count = 0;
            long client;
            int prev_request_num = 0;
            int cur_request_num = 0;
            long cur_time,prev_time = 0;
            while(true) {
            	if(count%15==0) {
            		//every 15 epoch, calculate current client interval to adjust the number of apptier and frontend servers
            		cur_time = System.currentTimeMillis();
            		prev_request_num = cur_request_num;
            		cur_request_num = get_request_num.get()+SL.getQueueLength();
            		if(cur_request_num>=prev_request_num&&count!=0) {
            			client = (cur_time-prev_time)/(cur_request_num-prev_request_num);
            			interval = (long)((double)client*3);
            		}           		
            		prev_time = cur_time;
            		
            	}
                while ((r = SL.getNextRequest()) == null) {
                }      	
            	AppQueue.add(r);
            	get_request_num.incrementAndGet();
                count++;
           
                dropAppImpossible();
                dropFrontImpossible();
                
                int[] machinechange = changeMachineNumber(interval,Apptier_machine_List.size(), Frontend_machine_List.size());
                
                if(machinechange[2]==1) {
                	//if change server number is set, change number to corresponding number(could be scale in or scale out)
                    setAppNum(machinechange[0]);
                    setFrontNum(machinechange[1]);
                }
            }
        }
        else {
            boolean role = true;
            try {        
                master_vm = (Iserver)LocateRegistry.getRegistry(cloud_ip, cloud_port).lookup("//127.0.0.1/no1");
                role = master_vm.getRole(vm_id);        
            }
            catch (Exception e){
            }
            if(role == false) {
            	//apptier work server
            	Icache = (Cloud.DatabaseOps) master_vm;
                while(true) {      
                	//before next processRequest(), check life status
                    Cloud.FrontEndOps.Request r = null;
                    check_appstatus();
                    try {
                        r = master_vm.pollAppQueue();            
                        SL.processRequest(r,Icache);
                        
                    }
                    catch (Exception e){
                        continue;
                    }
                }
            }
            else {
            	//frontend work server
                SL.register_frontend();
                Cloud.FrontEndOps.Request r = null;                
                while(true) {  
                	//before next getNextRequest(), check life status
                	check_frontstatus();
                    while ((r = SL.getNextRequest()) == null) {
                    }
                    master_vm.addAppQueue(r);
                    master_vm.addGetNum();
                }
            }
        }
    }
    
    public void addAppQueue(Cloud.FrontEndOps.Request r) throws RemoteException{
        /**
         * Invoked by frontend work servers to insert requests to Queue handled by it
         */ 
        AppQueue.add(r);
    }
    
    public Cloud.FrontEndOps.Request pollAppQueue() throws RemoteException{
        /**
         * Invoked by apptier work servers to get request to process in Queue
         * to get the role(frontend or apptier) of this server
         */ 
        Cloud.FrontEndOps.Request r = null;
        while (true){
            r = AppQueue.poll();
            if (r == null){
                continue;
            }
            else {    
                break;
            }
        }
        return r;
    }
    
    public void setRole(int vmid, boolean flag){
        /**
         * Invoked by frontend main server
         * to get the role(frontend or apptier) of this server
         */ 
        if(flag) {
            Frontend_machine_List.add(vmid);
        }
        else {
            Apptier_machine_List.add(vmid);
        }
    }
    
    public boolean getRole(int vmid) throws RemoteException{
        /**
         * Invoked by work server
         * to get the role(frontend or apptier) of this server
         */ 
        if(Frontend_machine_List.contains(vmid)) {
            return(true);
        }
        else {
            return(false);
        }
    }
    
    public static void initialization(long interval){
        /**
		 *	initialize frontend and apptier work servers according to given interval
         */ 
        int appmachine;
        int frontmachine;       
        if(interval>1500) {
            appmachine = 1;
            frontmachine = 0;
        }
        else if(interval<=1500 && interval>1000){
            appmachine = 2;
            frontmachine = 0;
        }
        else if(interval<=1000 && interval>400){
            appmachine = 3;
            frontmachine = 0;
        }
        else if(interval<=400 && interval>250){
            appmachine = 4;
            frontmachine = 0;
        }
        else if(interval<=250 && interval>200){
            appmachine = 5;
            frontmachine = 0;
        }
        else if(interval<=200 && interval>150){
            appmachine = 6;
            frontmachine = 1;
        }
        else {
            appmachine = 7;
            frontmachine = 1;
        }
        appmachine = appmachine - 1;
        for (int i = 0; i < appmachine; ++i) {
            Apptier_machine_List.add(SL.startVM());
        }
        for (int i = 0; i < frontmachine; ++i) {
            Frontend_machine_List.add(SL.startVM());
        }
        last_scale_timestamp = System.currentTimeMillis();
    }
    
    public void addAppmachine(int num){
        /**
		 *	add more num apptier servers
         */ 
        for (int i = 0; i < num; ++i) {
            Apptier_machine_List.add(SL.startVM());
        }
    }
    public void addFrontmachine(int num){
        /**
		 *	add more num frontend servers
         */ 
        for (int i = 0; i < num; ++i) {
            Frontend_machine_List.add(SL.startVM());
        }
    }
    
    public static void dropAppImpossible(){
        /**
		 *	drop requests received by frontend servers but impossible for current apptier servers to process on time
         */ 
        int worklength = SL.getQueueLength();
        int frontlength = Apptier_machine_List.size()+1;
        
        if(worklength> frontlength) {
            for (int i = 0; i < (worklength - frontlength)-1; ++i) {
                SL.dropHead();
            }
        }
    }
    
    public static void dropFrontImpossible(){
        /**
		 *	drop requests in queue before frontend layer which is impossible for current frontend servers to get request on time
         */ 
        int worklength = AppQueue.size();
        int applength = Apptier_machine_List.size();
        if(worklength> applength) {
            for (int i = 0; i < (worklength - applength)-1; ++i) {
                //System.out.println("drop!");
                SL.drop(AppQueue.poll());
            }
        }
    }
    
    public static int[] changeMachineNumber(long avg_client,int appmachine, int frontmachine){
        /**
         * Invoked by frontend main server
         * @param avg_interval the 
         * @param appmachine current number of apptier servers
         * @param frontmachine current number of frontend servers
         */ 
        int standard_appmachine;
        int standard_frontmachine;
        int[] ret = new int[3];
        
        if(System.currentTimeMillis() - last_scale_timestamp > Scale_threshold) {
        	ret[2] = 1;
            if(avg_client>1500) {
                standard_appmachine = 1;
                standard_frontmachine = 0;
            }
            else if(avg_client<=1500 && avg_client>1000){
                standard_appmachine = 2;
                standard_frontmachine = 0;
            }
            else if(avg_client<=1000 && avg_client>400){
                standard_appmachine = 3;
                standard_frontmachine = 0;
            }
            else if(avg_client<=400 && avg_client>250){
                standard_appmachine = 4;
                standard_frontmachine = 0;
            }
            else if(avg_client<=250 && avg_client>200){
                standard_appmachine = 5;
                standard_frontmachine = 0;
            }
            else if(avg_client<=200 && avg_client>150){
                standard_appmachine = 6;
                standard_frontmachine = 1;
            }
            else {
                standard_appmachine = 7;
                standard_frontmachine = 1;
            }
        }
        else {
        	ret[2] = 0;
        	standard_appmachine = appmachine;
        	standard_frontmachine = frontmachine;
        }
        ret[0] = standard_appmachine;
        ret[1] = standard_frontmachine;
        return(ret);
    }
    
	public void addGetNum() throws RemoteException{
        /**
         * Invoked by frontend main server
         * increase count request number atomically 
         */ 
		get_request_num.incrementAndGet();
	}
       
    public static void setAppNum(int AppNum){
        /**
         * Invoked by frontend main server
         * @param AppNum the number of apptier work servers you want to adjust to
         * modify current apptier work servers to given number 
         */ 
        int cur_AppNum = Apptier_machine_List.size();
        if(AppNum==cur_AppNum) {
        }
        else if(AppNum>cur_AppNum) {                
            for (int i = cur_AppNum; i <AppNum; ++i) {
                Apptier_machine_List.add(SL.startVM());
            }
            last_scale_timestamp = System.currentTimeMillis();
        }
        else {
        	synchronized (Lock) {
        		int index = Apptier_machine_List.size()-1;
	            for (int i = cur_AppNum; i >AppNum; --i) {
	                int remove_vmid = Apptier_machine_List.get(index);
	                index--;
	                killMark(remove_vmid);
	            }
        	}
        	last_scale_timestamp = System.currentTimeMillis();
        }
    }
    
    public static void setFrontNum(int FrontNum){
        /**
         * Invoked by frontend main server
         * @param FrontNum the number of frontend work servers you want to adjust to
         * modify current frontend work servers to given number 
         */ 
    	last_scale_timestamp = System.currentTimeMillis();
        int cur_FrontNum = Frontend_machine_List.size();//0 or 1
        if(FrontNum==cur_FrontNum) {
        }
        else if(FrontNum>cur_FrontNum) { 
            for (int i = cur_FrontNum; i <FrontNum; ++i) {
            	Frontend_machine_List.add(SL.startVM());
            }
            last_scale_timestamp = System.currentTimeMillis();
        }
        else {
        	synchronized (Lock) {
        		int index = Frontend_machine_List.size()-1;
	            for (int i = cur_FrontNum; i >FrontNum; --i) {
	                int remove_vmid = Frontend_machine_List.get(index);
	                index--;
	                killMark(remove_vmid);
	            }
        	}
        	last_scale_timestamp = System.currentTimeMillis();
        }
    }
    
    
    public static void killMark(int vmid){
        /**
         * Invoked by work server(server except frontend main server)
         * work servers receive signal from frontend main server to launch suicide procedure
         */ 
    	try{
	        Iserver Server2down = (Iserver)LocateRegistry.getRegistry(cloud_ip, cloud_port).lookup("//127.0.0.1/no"+ Integer.toString(vmid));
	        Server2down.receiveKillMark();
    	}
        catch (Exception e){
        }
    }
    public void killAction(int vmid) throws RemoteException{
        /**
         * Only Invoked by frontend main server when receive a kill signal from work server with its vmid(at this time, work server is asleep and wait for being teminated)
         * Kill all work server with given vmid
         */ 
    	synchronized (Lock) {
	    	System.out.println("endVM: " + vmid);
	    	int index = Apptier_machine_List.indexOf(vmid);
	    	Apptier_machine_List.remove(index);
	    	SL.endVM(vmid);
    	}
    }
    
    public void receiveKillMark() throws RemoteException{
        /**
         * Invoked by main work servers(server except frontend main server)
         * set work machine's machine_live_status to be false, launch kill procedure
         */ 
    	machine_live_status = false;
    }
    
    public static void check_appstatus(){
        /**
         * Invoked by apptier servers,
         * check live status, if alive do nothing, if not, connect to main frontend server to suicide, go to sleep and wait for being terminated 
         */ 
        if (!machine_live_status){
        	try {
            	master_vm.killAction(vm_id);
            	Thread.sleep(1000000);
        	}
            catch (Exception e){
            }
        }
    }
    
    public static void check_frontstatus(){
        /**
         * Invoked by frontend servers,
         * check live status, if alive do nothing, if not, connect to main frontend server to suicide, go to sleep and wait for being terminated 
         */ 
    	if (SL.getQueueLength() == 0){
	        if (!machine_live_status){
	        	try {
	        		SL.unregister_frontend();
	            	master_vm.killAction(vm_id);
	            	Thread.sleep(1000000);
	        	}
	            catch (Exception e){
	            }
	
	        }
    	}
    }
    
    public String get(String key) throws RemoteException{
        /**
         * Implements the Interface Cloud.DatabaseOps, override the original Database's get method to add cache
         */    
        String trim_key = key.trim();
        if (cache.containsKey(trim_key)){
        	//System.out.println("cache hit!");
            String value = cache.get(trim_key);
            return value;
        }
        else{
            String value = Database.get(key);
            cache.put(trim_key, value);
            return value;
        }
    }
    
    public synchronized boolean set(String key, String value, String password) throws RemoteException {
        /**
         * Implements the Interface Cloud.DatabaseOps, override the original Database's set method to add cache
         */     
        boolean ret = Database.set(key, value, password);
        if (ret){
            cache.put(key, value);
            return true;
        }
        
        return ret;
    }
    public synchronized boolean transaction(String item, float price, int qty) throws RemoteException {
        /**
         * Implements the Interface Cloud.DatabaseOps, override the original Database's transaction method to add cache
         */
        boolean ret = Database.transaction(item, price, qty);
        if (ret) {
            String trim_item = item.trim();
            String trim_item_qty = trim_item + "_qty";
            int new_qty = Integer.parseInt(cache.get(trim_item_qty))-qty;
            cache.put(trim_item_qty, String.valueOf(new_qty));
        }
        return ret;
    }
    
    
    
    
}
