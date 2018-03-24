package cht.client;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

public class ClientHandler extends SimpleChannelUpstreamHandler {
	
	private static final Logger logger = Logger.getLogger(
			ClientHandler.class.getSimpleName());
	
	public ClientHandler(){
		logger.setLevel(Level.WARNING);
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
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		String serverMsg = (String) e.getMessage();
		if ("exit".equals(serverMsg)) {
			e.getChannel().close(); // Server sent exit so close channel.
		} else {
		    System.out.println(e.getMessage()); // Simply print the received message from server.
		}
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
