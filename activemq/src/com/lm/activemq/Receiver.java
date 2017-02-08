package com.lm.activemq;

/** 
 * @Header: Receiver.java 
 * 类描述： 
 * @author: lm 
 * @date 2013-7-17 上午10:52:42 
 * @Email  
 * @company 欢 
 * @addr 北京市朝阳区劲松 
 */
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

public class Receiver {
	
	
	public static void main(String[] args) {
		// ConnectionFactory ：连接工厂，JMS 用它创建连接
		ConnectionFactory connectionFactory;

		// Connection ：JMS 客户端到JMS Provider 的连接
		Connection connection = null;

		// Session： 一个发送或接收消息的线程
		Session session;

		// Destination ：消息的目的地;消息发送给谁.
		Destination destination;

		// 消费者，消息接收者
		MessageConsumer consumer;

		// 构造ConnectionFactory实例对象，此处采用ActiveMq的实现jar
		connectionFactory = new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_USER, ActiveMQConnection.DEFAULT_PASSWORD, "tcp://localhost:61616");
		try {
			// 构造从工厂得到连接对象
			connection = connectionFactory.createConnection();

			// 启动
			connection.start();

			// 获取操作会话
			session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);

			// 获取session注意参数值xingbo.xu-queue是一个服务器的queue，须在在ActiveMq的console配置
			destination = session.createQueue("FirstQueue");
			
			// 得到消息消费者【接收者】
			consumer = session.createConsumer(destination);
			
			//接收消息
			receiveMessage(session, consumer);
			
			// 这里接收时session好像不用commit session.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != connection)
					connection.close();
			} catch (Throwable ignore) {
			}
		}
	}
	
	public static void receiveMessage(Session session, MessageConsumer consumer) throws Exception {
		while (true) {
			// 设置接收者接收消息的时间，为了便于测试，这里设定为100s
			TextMessage message = (TextMessage) consumer.receive(100 * 1000);
			if (null != message) {
				System.out.println("收到消息" + message.getText());
			} else {
				break;
			}
		}
	}
}