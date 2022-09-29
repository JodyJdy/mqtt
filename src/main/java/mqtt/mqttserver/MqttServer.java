

package mqtt.mqttserver;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import mqtt.codec.MqttDecoder;
import mqtt.codec.MqttEncoder;
import mqtt.codec.MqttServerMessageDecoder;
import mqtt.storage.*;


/**
 *mqtt 服务器
 **/

public class MqttServer {
    private final int port;

    public MqttServer(int port) {
        this.port = port;
    }

    public static void main(String[] args) throws Exception {
        new MqttServer(9999).start();
    }
    private void start() throws Exception{
        EventLoopGroup bossGroup = new NioEventLoopGroup(2);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            //存储服务端所有用户的会话信息
            UserSessions userSessions = new UserSessions();
            //中转接收到的所有消息
            MessageQueue queue = new MessageQueue();
            //用于将消息写入文件
            MessageStorage messageStorage = new MessageStorage();
            //用于写数据
            MessageWriter writer  = new MessageWriter(queue, messageStorage);
            //用于从文件读数据
            MessageFileReader reader = new MessageFileReader(messageStorage, userSessions);
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch)  {
                           ch.pipeline().addLast(new MqttEncoder());
                           ch.pipeline().addLast(new MqttDecoder());
                           ch.pipeline().addLast(new MqttServerMessageDecoder(userSessions,queue));
                        }
                    });
            // 启动消息处理
            writer.start();
            reader.start();
            ChannelFuture cf = bootstrap.bind(port).sync();
            cf.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
