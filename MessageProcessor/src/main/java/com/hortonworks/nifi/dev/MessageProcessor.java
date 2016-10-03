package com.hortonworks.nifi.dev;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.QueueConnection;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

public class MessageProcessor extends AbstractProcessor {
	private final class MessageOutputStreamCallback implements OutputStreamCallback {
		private TextMessage msgText;
		private MessageOutputStreamCallback(TextMessage msgText){
			this.msgText = msgText;
		}
		public void process(final OutputStream out) throws IOException {
			try {
				out.write(msgText.getText().getBytes());
			} catch (JMSException e) {
				e.printStackTrace();
			}
		}
	}
	private List<PropertyDescriptor> properties;
	private Set<Relationship> relationships;
	private QueueConnection qcon;
	private QueueSession qsession;
	private QueueReceiver qreceiver;

//	public static final AllowableValue QUEUE = new AllowableValue("QUEUE","QUEUE","Connect to a WebLogic Queue");
//	public static final AllowableValue TOPIC = new AllowableValue("TOPIC","TOPIC","Connect to a WebLogic Topic");
//	public static final PropertyDescriptor CONNECTION_TYPE = new PropertyDescriptor.Builder().name("Connection type")
//			.description("The type of WLS connection. (Topic or Queue)").required(true).allowableValues(QUEUE,TOPIC)
//			.defaultValue(TOPIC.getValue())
//			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	
	public static final PropertyDescriptor PROP_TOPIC_OR_QUEUE_NAME = new PropertyDescriptor.Builder().name("Topic/Queue Name")
			.description("The WLS Topic/Queue to pull messages from").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor PROP_JMS_FACTORY = new PropertyDescriptor.Builder().name("JMS Factory Name")
			.description("The JMS Factory Name").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor PROP_JNDI_FACTORY = new PropertyDescriptor.Builder()
			.name("JNDI Factory Name").description("The JNDI Factory Name").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor PROP_URL = new PropertyDescriptor.Builder().name("WLS Connection URL")
			.description("The WLS Connection URL").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor SECURITY_PRINCIPAL = new PropertyDescriptor.Builder().name("WLS Security Principal")
			.description("The WLS Security Principal (username)").required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor SECURITY_CREDENTIALS = new PropertyDescriptor.Builder().name("WLS Security Credentials")
			.description("The WLS Security Credentials (password)").sensitive(true).required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor QUEUE_RECEIVE_TIMEOUT = new PropertyDescriptor.Builder().name("Topic/Queue Receive Timeout")
			.description("The amount of time in milliseconds to wait for a message to be received.").defaultValue("15000").required(true)
			.addValidator(StandardValidators.LONG_VALIDATOR).build();
	public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
			.description("All FlowFiles that are received from the JMS Destination are routed to this relationship")
			.build();
	public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
			.description("Error in incoming data").build();

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
//		System.out.println("Inside OnTrigger=====================: ");
		try {
			processMessage(context, session);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void processMessage(ProcessContext context, ProcessSession session) throws Exception {
		InitialContext ctx = getInitialContext(context);
		ConnectionFactory connFactory = (ConnectionFactory) ctx.lookup(context.getProperty(PROP_JMS_FACTORY).getValue());
		Connection conn = connFactory.createConnection();
		Session jmsSession = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
		Destination destination = (Destination) ctx.lookup(context.getProperty(PROP_TOPIC_OR_QUEUE_NAME).getValue());
		MessageConsumer mc = jmsSession.createConsumer(destination);
		conn.start();

		TextMessage msgText;
		while (isScheduled() &&
				(msgText = (TextMessage) mc.receive(context.getProperty(QUEUE_RECEIVE_TIMEOUT).asLong())) != null) {
//			System.out.println("received: " + msgText.getText());
			FlowFile flowFile = session.create();
			flowFile = session.write(flowFile, new MessageOutputStreamCallback(msgText));
			session.getProvenanceReporter().receive(flowFile, context.getProperty("MyProp").getValue());
			session.transfer(flowFile, REL_SUCCESS);
			msgText.acknowledge();
			session.commit();
		}
		conn.close();
		
	}

	@Override
	protected void init(final ProcessorInitializationContext context) {
//		System.out.println("Inside Processor Init.");
		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);
		List<PropertyDescriptor> properties = new ArrayList<PropertyDescriptor>();
		properties.add(PROP_TOPIC_OR_QUEUE_NAME);
		properties.add(PROP_URL);
		properties.add(SECURITY_PRINCIPAL);
		properties.add(SECURITY_CREDENTIALS);
		properties.add(PROP_JMS_FACTORY);
		properties.add(PROP_JNDI_FACTORY);
		properties.add(QUEUE_RECEIVE_TIMEOUT);
		this.properties = Collections.unmodifiableList(properties);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return properties;
	}

	private static InitialContext getInitialContext(ProcessContext context) throws NamingException {
//		System.out.println("Inside InitialContext.");
		Hashtable<String, String> env = new Hashtable<String, String>();
		env.put(Context.INITIAL_CONTEXT_FACTORY, context.getProperty(PROP_JNDI_FACTORY).getValue());
		env.put(Context.PROVIDER_URL, context.getProperty(PROP_URL).getValue());
		if(context.getProperty(SECURITY_PRINCIPAL).isSet()){
			env.put(Context.SECURITY_PRINCIPAL, context.getProperty(SECURITY_PRINCIPAL).getValue());
		}
		if(context.getProperty(SECURITY_CREDENTIALS).isSet()){
			env.put(Context.SECURITY_CREDENTIALS, context.getProperty(SECURITY_CREDENTIALS).getValue());
		}
		return new InitialContext(env);
	}
	public void close() throws JMSException {
		qreceiver.close();
		qsession.close();
		qcon.close();
	}
}