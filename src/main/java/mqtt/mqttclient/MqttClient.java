

package mqtt.mqttclient;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import mqtt.codec.MqttClientMessageDecoder;
import mqtt.codec.MqttDecoder;
import mqtt.codec.MqttEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *mqtt 客户端
 **/
public class MqttClient {
    private static final Logger logger = LoggerFactory.getLogger(MqttClient.class);
    private final int port;
    private final String address;
    private Bootstrap bootstrap;
    private EventLoopGroup group;

    public MqttClient(int port, String address) {
        this.port = port;
        this.address = address;
        logger.info("准备连接到服务端，端口:{}, 地址:{}",port,address);
        createBootstrap();
    }

    private void createBootstrap(){
        EventLoopGroup group = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.remoteAddress(address, port);
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch)  {
                ch.pipeline().addLast(new MqttEncoder());
                ch.pipeline().addLast(new MqttDecoder());
                ch.pipeline().addLast(new MqttClientMessageDecoder());
            }
        });
        this.bootstrap = bootstrap;
        this.group = group;
    }

    public Publisher connect(MqttConnectOptions options){
        ChannelFuture future;
        try {
            future = bootstrap.connect().sync();
            Channel channel =future.channel();
            //消息发布
            final Publisher publisher = new Publisher(channel, new Ack());
            //消息处理
            final MessageExchanger messageExchanger = new MessageExchanger(publisher.getSubMap());
            channel.attr(MqttClientMessageDecoder.EXCHANGER).set(messageExchanger);
            channel.attr(MqttClientMessageDecoder.PUBLISHER).set(publisher);

            PublishResult connResult = publisher.sendConn(options);
            connResult.waitForAck();
            logger.info("连接成功");
            //启动一个定时心跳的任务
            final ScheduledThreadPoolExecutor beat = new ScheduledThreadPoolExecutor(1, r -> {
                Thread t = new Thread(r);
                t.setName("Heat-Beat");
                return t;
            });
            beat.scheduleAtFixedRate(publisher::sendPing,5,options.getKeepAliveTimeSeconds(), TimeUnit.SECONDS);
            //channel关闭后，停止相关线程
            channel.closeFuture().addListener((ChannelFutureListener) future1 -> {
                beat.shutdown();
                messageExchanger.shutDown();
                group.shutdownGracefully();
            });
            return publisher;
        } catch (InterruptedException e) {
            logger.error("连接失败");
        }
        return null;
    }
}
