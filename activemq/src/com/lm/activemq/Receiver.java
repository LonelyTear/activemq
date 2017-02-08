package com.lm.activemq;

/** 
 * @Header: Receiver.java 
 * �������� 
 * @author: lm 
 * @date 2013-7-17 ����10:52:42 
 * @Email  
 * @company �� 
 * @addr �����г��������� 
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
		// ConnectionFactory �����ӹ�����JMS ������������
		ConnectionFactory connectionFactory;

		// Connection ��JMS �ͻ��˵�JMS Provider ������
		Connection connection = null;

		// Session�� һ�����ͻ������Ϣ���߳�
		Session session;

		// Destination ����Ϣ��Ŀ�ĵ�;��Ϣ���͸�˭.
		Destination destination;

		// �����ߣ���Ϣ������
		MessageConsumer consumer;

		// ����ConnectionFactoryʵ�����󣬴˴�����ActiveMq��ʵ��jar
		connectionFactory = new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_USER, ActiveMQConnection.DEFAULT_PASSWORD, "tcp://localhost:61616");
		try {
			// ����ӹ����õ����Ӷ���
			connection = connectionFactory.createConnection();

			// ����
			connection.start();

			// ��ȡ�����Ự
			session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);

			// ��ȡsessionע�����ֵxingbo.xu-queue��һ����������queue��������ActiveMq��console����
			destination = session.createQueue("FirstQueue");
			
			// �õ���Ϣ�����ߡ������ߡ�
			consumer = session.createConsumer(destination);
			
			//������Ϣ
			receiveMessage(session, consumer);
			
			// �������ʱsession������commit session.commit();
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
			// ���ý����߽�����Ϣ��ʱ�䣬Ϊ�˱��ڲ��ԣ������趨Ϊ100s
			TextMessage message = (TextMessage) consumer.receive(100 * 1000);
			if (null != message) {
				System.out.println("�յ���Ϣ" + message.getText());
			} else {
				break;
			}
		}
	}
}