package varAfg01;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.jms.BytesMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class FileServer implements MessageListener {
	private static final String DESTINATION = "files";

	private String directory;
	private Connection mConnection;
	private Session mSession;
	private MessageConsumer mMessageConsumer;
	private MessageProducer mMessageProducer;
	private Destination mQueue;

	public FileServer(String directory) throws NamingException, JMSException {
		this.directory = directory;
		Context ctx = new InitialContext();
		ConnectionFactory mConnectionFactory = (ConnectionFactory) ctx.lookup("ConnectionFactory");
		mQueue = (Destination) ctx.lookup(DESTINATION);
		mConnection = mConnectionFactory.createConnection();
		mSession = mConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		mMessageConsumer = mSession.createConsumer(mQueue);
		mMessageConsumer.setMessageListener(this);
		mMessageProducer = mSession.createProducer(mQueue);
		mConnection.start();
		System.out.println("FileServer gestartet ...");
	}

	@Override
	public void onMessage(Message message) {
		try {
			if (message instanceof TextMessage) {
				TextMessage receivedTextMessage = (TextMessage) message;
				System.out.println("Empfangen von Requestor:");
				System.out.println("   Inhalt:       " + receivedTextMessage.getText());
				System.out.println("   Priorität:    " + receivedTextMessage.getStringProperty("Priority"));
				System.out.println("   Antwort an:   " + receivedTextMessage.getJMSReplyTo());

				if (receivedTextMessage != null) {
					Destination replyQueue = (Destination) receivedTextMessage.getJMSReplyTo();

					if (replyQueue != null) {
						BytesMessage replyBytesMessage = mSession.createBytesMessage();
						File file = new File(directory + "/" + receivedTextMessage.getText());
						Boolean fileExists = false;

						if (file.isFile()) {
							fileExists = true;
						}
						replyBytesMessage.setBooleanProperty("status", fileExists);
						System.out.println("Gewünschte Datei vorhanden: " + fileExists);

						try { // Datei in reply packen
							InputStream in = new FileInputStream(file.getPath());
							int c;
							while ((c = in.read()) != -1) {
								replyBytesMessage.writeByte((byte) c);
							}
							in.close();
						} catch (IOException e) {
							System.err.println("Datei nicht lesbar.");
							// weitere Behandlungen einfügen
						}
						mMessageProducer = mSession.createProducer(replyQueue);
						mMessageProducer.send(replyBytesMessage);
						mMessageProducer.close();
						System.out.println("BytesMessage an Requestor übertragen \n");
					}
				}
			}
		} catch (JMSException e) {
			System.err.println(e);
		}
	}

	public void close() {
		try {
			if (mMessageProducer != null)
				mMessageProducer.close();
			if (mMessageConsumer != null)
				mMessageConsumer.close();
			if (mSession != null)
				mSession.close();
			if (mConnection != null)
				mConnection.close();
		} catch (JMSException e) {
			System.err.println(e);
		}
	}

	public static void main(String[] args) throws Exception {
		FileServer server = null;
		try {
			long wait = 10000000;
			server = new FileServer(args[0]);
			Thread.sleep(wait);
		} catch (Exception e) {
			System.err.println(e);
		} finally {
			if (server != null)
				server.close();
		}
	}
}
