/* Skeleton code for Server */

public class Server implements ProjectLib.CommitServing {
	
	public void startCommit( String filename, byte[] img, String[] sources ) {
		System.out.println( "Server: Got request to commit "+filename );
	}
	
	public static void main ( String args[] ) throws Exception {
		if (args.length != 1) throw new Exception("Need 1 arg: <port>");
		Server srv = new Server();
		ProjectLib PL = new ProjectLib( Integer.parseInt(args[0]), srv );
		
		// main loop
		while (true) {
			ProjectLib.Message msg = PL.getMessage();
			System.out.println( "Server: Got message from " + msg.addr );
			System.out.println( "Server: Echoing message to " + msg.addr );
			PL.sendMessage( msg );
		}
	}
}

