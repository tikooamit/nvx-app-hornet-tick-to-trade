/**
 * Copyright (c) 2015 Neeve Research & Consulting LLC. All Rights Reserved.
 * Confidential and proprietary information of Neeve Research & Consulting LLC.
 * CopyrightVersion 1.0
 */
package com.neeve.tick2trade;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Before;
import org.junit.BeforeClass;

import com.neeve.aep.AepEngine;
import com.neeve.ci.XRuntime;
import com.neeve.config.VMConfigurer;
import com.neeve.test.UnitTest;
import com.neeve.tick2trade.driver.Client;
import com.neeve.tick2trade.driver.Market;
import com.neeve.server.Configurer;
import com.neeve.server.embedded.EmbeddedServer;
import com.neeve.util.UtlStr;
import com.neeve.util.UtlStr.ISubstResolver;

/**
 * Base class with some helper methods for creating embedded servers. 
 */
public class AbstractAppTest extends UnitTest {
    private static volatile boolean vmConfigured = false;
    private static final String desktopConf = "conf/desktop/application.conf";

    @BeforeClass
    public static void unitTestIntialize() throws IOException {
        System.setProperty("nv.app.propfile", new File(getProjectBaseDirectory(), desktopConf).toString());
        UnitTest.unitTestIntialize();
    }

    @Before
    public void beforeTestcase() {
        vmConfigured = false;
    }

    @Before
    public void afterTestcase() {
        vmConfigured = false;
    }

    private static class PocConfigurer implements Configurer, ISubstResolver {
        private final String serverName;
        private final ISubstResolver envResolver = new UtlStr.SubstResolverFromEnv();
        final Map<String, String> overrides = new HashMap<String, String>();

        PocConfigurer(final String serverName, Map<String, String> configOverrides) {
            this.serverName = serverName;
            Properties props = new Properties();
            try {
                props.load(new FileInputStream(new File(getProjectBaseDirectory(), desktopConf)));
            }
            catch (IOException e) {
                throw new RuntimeException("Error loading embedded test properties", e);
            }
            for (Object prop : props.keySet()) {
                overrides.put(prop.toString(), props.getProperty(prop.toString()));
                System.setProperty(prop.toString(), props.getProperty(prop.toString()));
            }
            XRuntime.updateProps(props);
        }

        /* (non-Javadoc)
         * @see com.neeve.server.Configurer#configure(java.lang.String[])
         */
        @Override
        public String[] configure(String[] args) throws Exception {
            if (!vmConfigured) {
                URL overlayUrl = new File(getProjectBaseDirectory(), "conf/platform.xml").toURI().toURL();
                File overlayConfig = new File(overlayUrl.toURI());
                VMConfigurer.configure(overlayConfig, this);
                vmConfigured = true;
            }
            return new String[] { "--name", serverName };
        }

        /* (non-Javadoc)
         * @see com.neeve.util.UtlTailoring.PropertySource#getValue(java.lang.String, java.lang.String)
         */
        @Override
        public String getValue(String key, String defaultValue) {
            String override = overrides.get(key);
            if (override != null) {
                return override;
            }
            return envResolver.getValue(key, defaultValue);
        }
    }

    public App startEmsPrimary() throws Throwable {
        PocConfigurer configurer = new PocConfigurer("ems1", null);
        EmbeddedServer server = EmbeddedServer.create(configurer);
        server.start();
        return (App)server.getApplication("ems");
    }

    public App startEmsBackup() throws Throwable {
        PocConfigurer configurer = new PocConfigurer("ems2", null);
        EmbeddedServer server = EmbeddedServer.create(configurer);
        server.start();
        return (App)server.getApplication("ems");
    }

    public Market startMarket() throws Throwable {
        PocConfigurer configurer = new PocConfigurer("market", null);
        EmbeddedServer server = EmbeddedServer.create(configurer);
        server.start();
        return (Market)server.getApplication("market");
    }

    public Client startClient() throws Throwable {
        PocConfigurer configurer = new PocConfigurer("client", null);
        EmbeddedServer server = EmbeddedServer.create(configurer);
        server.start();
        return (Client)server.getApplication("client");
    }

    final protected void waitForTransactionPipelineToEmpty(final AepEngine engine) throws Exception {
        int i;
        for (i = 0; i < 100; i++) {
            final long numCommitsPending = (engine.getStats().getNumCommitsStarted() - engine.getStats().getNumCommitsCompleted());
            if (numCommitsPending == 0l) {
                break;
            }
            else {
                System.out.println("Waiting for transaction pipeline to empty remaining: " + numCommitsPending);
                Thread.sleep(100l);
            }
        }
        assertTrue(i < 100);
    }
}
