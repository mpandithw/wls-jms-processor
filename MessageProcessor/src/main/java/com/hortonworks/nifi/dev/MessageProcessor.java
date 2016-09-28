package com.hortonworks.nifi.dev;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
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
	private List<PropertyDescriptor> properties;
	private Set<Relationship> relationships;
	private QueueConnection qcon;
	private QueueSession qsession;
	private QueueReceiver qreceiver;

	public static final PropertyDescriptor PROP_QUEUE = new PropertyDescriptor.Builder().name("Queue Name")
			.description("The WLS Queue to pull messages from").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor PROP_JMS_FACTORY = new PropertyDescriptor.Builder().name("JMS Factory Name")
			.description("The WLS Queue to pull messages from").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor PROP_JNDI_FACTORY = new PropertyDescriptor.Builder()
			.name("JNDI Factory Name").description("The WLS Queue to pull messages from").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor PROP_URL = new PropertyDescriptor.Builder().name("WLS Connection URL")
			.description("The WLS Queue to pull messages from").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor SECURITY_PRINCIPAL = new PropertyDescriptor.Builder().name("WLS Security Principal")
			.description("The WLS Security Principal (username)").required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor SECURITY_CREDENTIALS = new PropertyDescriptor.Builder().name("WLS Security Credentials")
			.description("The WLS Security Credentials (password)").sensitive(true).required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
			.description("All FlowFiles that are received from the JMS Destination are routed to this relationship")
			.build();
	public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
			.description("Error in incoming data").build();

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		System.out.println("Inside OnTrigger=====================: ");
		try {
			processMessage(context, session);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void processMessage(ProcessContext context, ProcessSession session) throws Exception {
		final TextMessage msgText;
		InitialContext ctx = getInitialContext(context);
		Queue queue = (Queue) ctx.lookup(context.getProperty(PROP_QUEUE).getValue());
		QueueConnectionFactory connFactory = (QueueConnectionFactory) ctx
				.lookup(context.getProperty(PROP_JMS_FACTORY).getValue());
		QueueConnection queueConn = connFactory.createQueueConnection();
		QueueSession queueSession = queueConn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		QueueReceiver queueReceiver = queueSession.createReceiver(queue);
		queueConn.start();
		msgText = (TextMessage) queueReceiver.receive();

		System.out.println("received: " + msgText.getText());

		if (msgText != null) {

			FlowFile flowFile = session.create();
			flowFile = session.write(flowFile, new OutputStreamCallback() {

				public void process(final OutputStream out) throws IOException {
					try {
						out.write(msgText.getText().getBytes());
					} catch (JMSException e) {
						e.printStackTrace();
					}
				}
			});
			session.getProvenanceReporter().receive(flowFile, context.getProperty("MyProp").getValue());
			session.transfer(flowFile, REL_SUCCESS);
		}
		queueConn.close();
		session.commit();
	}

	@Override
	protected void init(final ProcessorInitializationContext context) {
		System.out.println("Inside Processor Init.");
		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);
		List<PropertyDescriptor> properties = new ArrayList<PropertyDescriptor>();
		properties.add(PROP_QUEUE);
		properties.add(PROP_URL);
		properties.add(SECURITY_PRINCIPAL);
		properties.add(SECURITY_CREDENTIALS);
		properties.add(PROP_JMS_FACTORY);
		properties.add(PROP_JNDI_FACTORY);
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
		System.out.println("Inside InitialContext.");
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