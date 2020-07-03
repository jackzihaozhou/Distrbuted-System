import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Iserver extends Remote, Cloud.DatabaseOps{
	
	public void addAppQueue(Cloud.FrontEndOps.Request r) throws RemoteException;
	public Cloud.FrontEndOps.Request pollAppQueue() throws RemoteException;
	public boolean getRole(int vmid) throws RemoteException;
	public void receiveKillMark() throws RemoteException;
	public void killAction(int vmid) throws RemoteException;
	public void addGetNum() throws RemoteException;	
}