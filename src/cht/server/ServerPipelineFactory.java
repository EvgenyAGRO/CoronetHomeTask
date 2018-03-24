package cht.server;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import static org.jboss.netty.channel.Channels.*;

public class ServerPipelineFactory implements ChannelPipelineFactory{
	
	private ServerHandler _serverHandler;
	
	public ServerPipelineFactory(String filePathForPersistance){
		_serverHandler = new ServerHandler(filePathForPersistance);
	}
	
	@Override
	public ChannelPipeline getPipeline() throws Exception {
		ChannelPipeline pipeline =  pipeline();

		// Add the text line codec.
		pipeline.addLast("framer", new DelimiterBasedFrameDecoder(
		        8192, Delimiters.lineDelimiter()));
		pipeline.addLast("decoder", new StringDecoder());
		pipeline.addLast("encoder", new StringEncoder());
		
		// Add server logic
		pipeline.addLast("handler", _serverHandler);
		return pipeline;
	}
	
	public ServerHandler getServerHandler(){
		return _serverHandler;
	}
}
