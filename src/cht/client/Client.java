package cht.client;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

/**
* The program implements client application that connects to the server and may send various commands
* using a predefined protocol.
*
* @author  Evgeny Agronsky
* @version 1.0
* @since   2018-03-24 
*/
public class Client {

	public static class ClientConfiguration{
		private String _host;
		private int _port;
		
		public ClientConfiguration(String host, int port){
			_host = host;
			_port = port;
		}

		public String getHost() {
			return _host;
		}

		public int getPort() {
			return _port;
		}
	}
	
	private static final String DEFAULT_HOST = "localhost";
	private static final int DEFAULT_PORT_NUMBER = 9999;
	private ClientConfiguration _clientConfig;
	
	public Client(ClientConfiguration clientConf){
		_clientConfig = clientConf;
	}
	
	/**
	 * Function describing the protocol between the client and the server
	 * */
	public void printHelp(){
		System.out.println("---Welcome to CHT client application---\n\n"
				+ "The available commands are:\n"
				+ "getallkeys_<pattern> - to receive all keys matching the specified pattern, example: getallkeys_abc\n"
				+ "rightadd_<K>_<V> - to add a value V to key K, from the right, example: rightadd_abc_123\n"
				+ "leftadd_<K>_<V> - to add a value V to key K, from the left, example: leftadd_abc_123\n"
				+ "set_<K>_<[V]> - to add a pair of key K with values list [V] separated by comma, example: set_abc_1,2,3\n"
				+ "get_<K> - to get a values list by key K, example: get_abc\n\n"
				+ "Type 'help' for option menu and 'exit' to quit");
	}
	
	/**
	 * Checking the validity of the command on client side
	 * */
	public boolean isCmdValid(String cmd){
		String[] parsedCmd = cmd.split("_");
	
		if (parsedCmd.length == 1){
			
			if (!(parsedCmd[0].equals("exit"))){
				return false;
			}
			
		} else if (parsedCmd.length == 2){
			
			if (!(parsedCmd[0].equals("get") || parsedCmd[0].equals("getallkeys"))){
				return false;
			}
			
		} else if (parsedCmd.length == 3){
			
			if (!(parsedCmd[0].equals("set") || parsedCmd[0].equals("rightadd") || parsedCmd[0].equals("leftadd"))){
				return false;
			}
			
		} else {
	    	System.err.println("Avoid using _ as part of your keys/values");
			return false;
		}
		return true;
	}
	
	
	/**
	 * Start the client and prompt user for commands
	 * */
	public void startClient() throws IOException{
		// Configure the client.
		ClientBootstrap bootstrap = new ClientBootstrap(
		        new NioClientSocketChannelFactory(
		                Executors.newCachedThreadPool(),
		                Executors.newCachedThreadPool()));
		
		// Configure the pipeline factory.
		bootstrap.setPipelineFactory(new ClientPipelineFactory());
		
		// Start the connection attempt.
		ChannelFuture future = bootstrap.connect(new InetSocketAddress(_clientConfig.getHost(), _clientConfig.getPort()));
		
		// Wait until the connection attempt succeeds or fails.
		Channel channel = future.awaitUninterruptibly().getChannel();
		if (!future.isSuccess()) {
			System.err.println("Client can not connect to server. Exiting.");
		    bootstrap.releaseExternalResources();
		    return;
		}
		
		// Read commands from the stdin.
		ChannelFuture lastWriteFuture = null;
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		printHelp();
		for (;;) {
		    String cmd = in.readLine();
		    
		    if (cmd == null) {
		    	//End of stream has been reached.
		        break;
		    }
		    else if (!channel.isConnected()){ //server was disconnected
		    	System.out.println("Server channel was disconnected. Exiting...");
		        break;
		    }
		    // If user typed the 'exit' command, wait until the server closes the connection.
		    else if (cmd.toLowerCase().equals("exit")) {
		    	System.out.println("Client is terminating...");
		        break;
		    }
		    
		    // If empty cmd just ignore.
		    if (cmd.isEmpty()){
		    	continue;
		    }
		    else if (cmd.toLowerCase().equals("help")){
		    	printHelp();
		    	continue;
		    }
		    else if (isCmdValid(cmd)){
			    // Sends the received valid cmd to the server.
			    lastWriteFuture = channel.write(cmd + "\r\n");
		    } else {
		    	System.err.println("The following command is invalid: " + cmd);
		    	printHelp();
		    	continue;
		    }
		}
		
		// Wait until all messages are flushed before closing the channel.
		if (lastWriteFuture != null) {
		    lastWriteFuture.awaitUninterruptibly();
		}
		
		// Close the connection.  Make sure the close operation ends because
		// all I/O operations are asynchronous in Netty.
	    channel.close().awaitUninterruptibly();
	
	    // Shut down all thread pools to exit.
	    bootstrap.releaseExternalResources();
	}
	
	/**
	 * Parse program commands
	 * */
	private static CommandLine getParsedArgs(String[] args) {
	     Options options = new Options();
	
		 Option portOpt = new Option("p", "port", true, "host port of the server to connect to");
		 portOpt.setType(Integer.class);
		 options.addOption(portOpt);
		
		 Option hostOpt = new Option("h", "host", true, "host ip of the server to connect to");
		 options.addOption(hostOpt);
		
		
		 CommandLineParser parser = new DefaultParser();
		 HelpFormatter formatter = new HelpFormatter();
		 CommandLine cmd = null;
		 
		 
		 try {
	        cmd = parser.parse(options, args);
	     } catch (ParseException e) {
			System.err.println(e.getMessage());
			formatter.printHelp(Client.class.getSimpleName(), options);
			System.exit(-1);
	     }
	     return cmd;
	}
	
	
	public static void main(String[] args) {
		CommandLine parsedArgs = getParsedArgs(args);
		int port = 0;
		try {
			port = Integer.parseInt(parsedArgs.getOptionValue("port", String.valueOf(DEFAULT_PORT_NUMBER)));
		} catch (NumberFormatException e) {
			System.err.println("Port number must be an integer.");
			return;
		}
		
		// InetSocketAddress class already takes care of proper port numbers and ip formats so no need to check it twice
		Client newClient = new Client(new ClientConfiguration(parsedArgs.getOptionValue("host", DEFAULT_HOST), port));
		try {
			newClient.startClient();
		} catch (IOException e) {
			System.err.println("Client failed to start. Exiting.");
			System.err.println("Cause: " + e.getMessage());
		}
	}
}
