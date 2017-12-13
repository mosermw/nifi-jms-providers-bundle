package com.mosermw.nifi.jms.cf;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.jms.cf.JMSConnectionFactoryProviderDefinition;
import org.apache.nifi.ssl.SSLContextService;

import javax.jms.ConnectionFactory;
import javax.jms.JMSRuntimeException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Tags({ "jms", "messaging", "queue", "topic", "publish", "subscribe", "activemq" })
@CapabilityDescription("This Apache NiFi service creates a javax.jms.ConnectionFactory implementation that supports ActiveMQ."
+ "It may be reused by all Apache NiFi components that connect to the same ActiveMQ JMS server.")
@SeeAlso(classNames = { "org.apache.nifi.jms.processors.ConsumeJMS", "org.apache.nifi.jms.processors.PublishJMS"})
public class ActiveMQJMSConnectionFactoryProvider extends AbstractControllerService implements JMSConnectionFactoryProviderDefinition {

    private static final List<PropertyDescriptor> propertyDescriptors;

    private volatile ConnectionFactory factory;

    static {
        propertyDescriptors = Collections.unmodifiableList(Arrays.asList(
                JMSConnectionFactoryProviderDefinition.BROKER_URI,
                JMSConnectionFactoryProviderDefinition.SSL_CONTEXT_SERVICE
        ));
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public ConnectionFactory getConnectionFactory() {
        if (factory == null) {
            throw new IllegalStateException("ConnectionFactory is not available, this service has not been completely enabled");
        }
        return factory;
    }

    @OnEnabled
    public void enable(ConfigurationContext context) {
        final String brokerUri = context.getProperty(BROKER_URI).evaluateAttributeExpressions().getValue();
        final SSLContextService sslContext = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

        ActiveMQConnectionFactory factory;
        try {
            if (sslContext != null) {
                final ActiveMQSslConnectionFactory sslFactory = new ActiveMQSslConnectionFactory(brokerUri);
                sslFactory.setKeyStore(sslContext.getKeyStoreFile());
                sslFactory.setKeyStoreType(sslContext.getKeyStoreType());
                sslFactory.setKeyStorePassword(sslContext.getKeyStorePassword());
                sslFactory.setKeyStoreKeyPassword(sslContext.getKeyPassword());
                sslFactory.setTrustStore(sslContext.getTrustStoreFile());
                sslFactory.setTrustStoreType(sslContext.getTrustStoreType());
                sslFactory.setTrustStorePassword(sslContext.getTrustStorePassword());
                factory = sslFactory;
            } else {
                factory = new ActiveMQConnectionFactory(brokerUri);
            }
        } catch (Exception e) {
            throw new JMSRuntimeException("Failed setup of ActiveMQConnectionFactory", "0", e);
        }

        this.factory = factory;
    }

    @OnDisabled
    public void disable() {
        this.factory = null;
    }
}
