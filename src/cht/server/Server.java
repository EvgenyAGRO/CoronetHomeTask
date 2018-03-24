package cht.server;
import java.io.BufferedReader;
import java.io.File;
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
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

/**
* The program implements server application that starts a server and may serve multiple clients
* with data using a predefined protocol.
*
* @author  Evgeny Agronsky
* @version 1.0
* @since   2018-03-24 
*/
public class Server {
	
	public static class ServerConfiguration{
		private String _host;
		private String _filePath;
		private int _port;
		
		public ServerConfiguration(String host, int port, String filepathToPersist){
			_host = host;
			_port = port;
			_filePath = filepathToPersist;
		}

		public String getHost() {
			return _host;
		}
		
		public String getFilepathToPersist() {
			return _filePath;
		}

		public int getPort() {
			return _port;
		}
	}
	
	private static final String DEFAULT_HOSTNAME = "localhost";
	private static final String DEFAULT_DATA_PERSISTANCE_PATH = "data.ser";
	private static final int DEFAULT_PORT_NUMBER = 9999;
	
	private ServerConfiguration _serverConfig;
	private ServerPipelineFactory _serverPipelineFactory;
	
	public Server(ServerConfiguration serverConf){
		_serverConfig = serverConf;
	}
	
	
	/**
	 * Start server with specified host and port
	 * @throws IOException 
	 * @throws InterruptedException 
	 * */
	public void startServer() throws IOException {
		// Create new file/make sure it exists for data persistence.
		File fileToPersistTo = new File(_serverConfig.getFilepathToPersist());
		fileToPersistTo.createNewFile(); 
		
		// Configure the server.
		ServerBootstrap bootstrap = new ServerBootstrap(
		        new NioServerSocketChannelFactory(
		                Executors.newCachedThreadPool(),
		                Executors.newCachedThreadPool()));
		
		// Configure the pipeline factory.
		_serverPipelineFactory = new ServerPipelineFactory(_serverConfig.getFilepathToPersist());
		bootstrap.setPipelineFactory(_serverPipelineFactory);
		
		// Bind and start to accept incoming connections.
		Channel serverChannel = bootstrap.bind(new InetSocketAddress(_serverConfig.getHost(), _serverConfig.getPort()));
		
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		ChannelFuture lastWriteFuture = null;
		printHelp();
		for (;;) {
		    String cmd = in.readLine();
		    if (cmd == null) {
		    	//End of stream has been reached.
		        break;
		    }
		    
		    if (cmd.toLowerCase().equals("help")){
		    	printHelp();
		    	continue;
		    }
		    else if (cmd.toLowerCase().equals("exit")){
			    lastWriteFuture = serverChannel.write(cmd + "\r\n");
			    System.out.println("Server is terminating...");
			    serverChannel.close();
		    	break;
		    }  
		}
		
		// Wait until all messages are flushed before closing the channel.
		if (lastWriteFuture != null) {
		    lastWriteFuture.awaitUninterruptibly();
		}
		
		// Wait until the server socket is closed.
		try {
			serverChannel.getCloseFuture().sync();
		} catch (InterruptedException e) {
			System.err.println("Waiting for socket closing was interrupted.");
			System.err.println("Cause: " + e.getMessage());
		}
		
		// Persist data before exiting.
		_serverPipelineFactory.getServerHandler().getServerCache().stopThreadAndPersistData();
		bootstrap.releaseExternalResources();
	}
	
	private void printHelp() {
		System.out.println("---Welcome to CHT server application---\n\n"
				+ "Type 'help' for option menu and 'exit' to quit");
	}


	/**
	 * Parse program commands
	 * */
	private static CommandLine getParsedArgs(String[] args) {
	     Options options = new Options();
	
		 Option portOpt = new Option("p", "port", true, "host port of the server to connect to");
		 portOpt.setType(Integer.class);
		 options.addOption(portOpt);
		 
		 Option fileOpt = new Option("f", "file", true, "file path to persist data to");
		 options.addOption(fileOpt);
		
		 Option hostOpt = new Option("h", "host", true, "host ip of the server to connect to");
		 options.addOption(hostOpt);
		
		
		 CommandLineParser parser = new DefaultParser();
		 HelpFormatter formatter = new HelpFormatter();
		 CommandLine cmd = null;
		 
		 
		 try {
	        cmd = parser.parse(options, args);
	     } catch (ParseException e) {
			System.err.println(e.getMessage());
			formatter.printHelp(Server.class.getSimpleName(), options);
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
		Server newServ = new Server(new ServerConfiguration(parsedArgs.getOptionValue("host", DEFAULT_HOSTNAME), port, 
				parsedArgs.getOptionValue("file", DEFAULT_DATA_PERSISTANCE_PATH)));
		try {
			newServ.startServer();
		} catch (IOException e) {
			System.err.println("Server failed to start. Exiting.");
			System.err.println("Cause: " + e.getMessage());
		}
	}
}
