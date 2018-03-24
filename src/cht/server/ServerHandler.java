package cht.server;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import cht.cache.LRUPersistentCache;

public class ServerHandler extends SimpleChannelUpstreamHandler {
	private static final String CLIENT_TERMINATE_MSG = "Goodbye!";
	private static final Logger logger = Logger.getLogger(
			ServerHandler.class.getSimpleName());
	
	private LRUPersistentCache _serverCache;
	
	public ServerHandler(String filePathForPersistance) {
		_serverCache = new LRUPersistentCache(filePathForPersistance);
		_serverCache.start();
		logger.setLevel(Level.WARNING);
	}
	
	public LRUPersistentCache getServerCache(){
		return _serverCache;
	}
	
	private String parseAndExecuteCommand(String command, Channel currChannel){
		
		String[] parsedCommand = command.split("_");
		
	    // Close the connection if the client has sent exit.
	    if (command.toLowerCase().equals("exit")) {
	    	currChannel.close();
	    	return CLIENT_TERMINATE_MSG;
	    }
	    else if (parsedCommand.length == 2){
	    	if (parsedCommand[0].equals("getallkeys")){ // It's a getallkeys command
	    		Set<String> allKeys = _serverCache.getAllKeys(parsedCommand[1]);
	    		return allKeys.isEmpty() ? "No keys are avaliable for pattern: " + parsedCommand[1] : String.join(",", allKeys);
	    		
	    	} else { // It's a get command
	    		List<String> keyValue = _serverCache.get(parsedCommand[1]);
	    		return keyValue == null ? "Following key does not exist: " + parsedCommand[1] : String.join(",", keyValue); 
	    	}
	    }
	    else{ // Parsed command has 3 parts.
	    	if (parsedCommand[0].equals("rightadd")){ // It's a rightadd command
	    		_serverCache.rightAdd(parsedCommand[1], parsedCommand[2]);
	    		return "Right add of val " + parsedCommand[2] + " to key " + parsedCommand[1] + " was done successfully.";
	    	} 
	    	else if (parsedCommand[0].equals("leftadd")) { // It's a leftadd command
	    		_serverCache.leftAdd(parsedCommand[1], parsedCommand[2]);
	    		return "Left add of val " + parsedCommand[2] + " to key " + parsedCommand[1] + " was done successfully.";
	    	} 
	    	else { // It's a set command
	    		String key = parsedCommand[1];
	    		List<String> valuesList = Arrays.asList(parsedCommand[2].split(","));
	    		_serverCache.set(key, valuesList);
	    		return "List of values [" + parsedCommand[2] + "] was associated with key " + parsedCommand[1] + " successfully.";
	    	}
	    }
	}
	
	
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
	    // Convert to a String first.
	    String command = (String) e.getMessage();
	    Channel currChannel = e.getChannel();
   
	    // Get command output.
	    String commandOutput = parseAndExecuteCommand(command, currChannel);
	    if (commandOutput != null){
	    	currChannel.write(commandOutput + '\n');
	    }
	}
	
	
	@Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        // Log all channel state changes.
        if (e instanceof ChannelStateEvent) {
            logger.info("Channel state changed: " + e);
        }
        super.handleUpstream(ctx, e);
    }
	
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
	   logger.log(
	           Level.WARNING,
	           "Unexpected exception from downstream.",
	           e.getCause());
	   e.getChannel().close();
	}
}
