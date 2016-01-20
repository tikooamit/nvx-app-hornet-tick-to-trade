/**
 * Copyright (c) 2015 Neeve Research & Consulting LLC. All Rights Reserved.
 * Confidential and proprietary information of Neeve Research & Consulting LLC.
 * CopyrightVersion 1.0
 */
package com.neeve.tick2trade;

import org.junit.Test;

import static org.junit.Assert.*;

import com.neeve.ci.XRuntime;
import com.neeve.tick2trade.driver.Market;

/**
 * Tests message flow by starting embedded servers. 
 */
public class TestMessageFlow extends AbstractAppTest {
    @Test
    public void testMessageFlow() throws Throwable {
        App primary = startEmsPrimary();
        Market market;
        market = startMarket();
        startClient();

        int expectedSendCount = XRuntime.getValue("simulator.manualSendCount", 0);
        int slicesPerOrder = XRuntime.getValue("simulator.sor.slicesPerOrder", 4);
        while (!primary.getEms().done(expectedSendCount)) {
            Thread.sleep(500);
        }

        System.out.println("Waiting for Market Slices");
        while (!market.done(expectedSendCount * slicesPerOrder)) {
            Thread.sleep(500);
        }

        waitForTransactionPipelineToEmpty(primary.getEngine());

        assertEquals("Unexpected number of OMSNewOrderSingles", expectedSendCount, primary.getEms().getRcvdNosCount());

        Thread.sleep(1000);
        if (primary.getEngine().getStore() != null && primary.getEngine().getStore().getPersister() != null) {
            primary.getEngine().getStore().getPersister().sync();
        }
        if (primary.getEngine().getOutboundMessageLogger() != null) {
            primary.getEngine().getOutboundMessageLogger().flush(true);
        }

        Thread.sleep(1000);
    }
}
