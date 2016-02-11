package com.neeve.tick2trade.driver;

import java.io.File;
import java.net.URL;
import java.util.Set;

import cern.colt.function.IntObjectProcedure;

import com.neeve.aep.AepEngine.HAPolicy;
import com.neeve.aep.AepEngine;
import com.neeve.aep.AepEngineDescriptor;
import com.neeve.aep.IAepBusManagerStats;
import com.neeve.aep.IAepEngineStats.IMessageTypeStats;
import com.neeve.aep.annotations.EventHandler;
import com.neeve.ci.XRuntime;
import com.neeve.cli.annotations.Command;
import com.neeve.cli.annotations.Option;
import com.neeve.lang.XString;
import com.neeve.rog.IRogMessage;
import com.neeve.root.RootConfig;
import com.neeve.server.app.annotations.AppHAPolicy;
import com.neeve.server.app.annotations.AppStat;
import com.neeve.server.app.annotations.AppVersion;
import com.neeve.sma.MessageChannel.Qos;
import com.neeve.stats.IStats.Counter;
import com.neeve.stats.IStats.Latencies;
import com.neeve.stats.StatsFactory;
import com.neeve.tick2trade.acl.MarketOrderNewPopulator;
import com.neeve.tick2trade.acl.MarketTradePopulator;
import com.neeve.tick2trade.messages.MarketNewOrderSingle;
import com.neeve.tick2trade.messages.MarketOrderNew;
import com.neeve.tick2trade.messages.MarketTrade;
import com.neeve.tick2trade.messages.WarmupComplete;
import com.neeve.toa.DefaultServiceDefinitionLocator;
import com.neeve.toa.TopicOrientedApplication;
import com.neeve.toa.service.ToaService;
import com.neeve.toa.service.ToaServiceChannel;
import com.neeve.toa.spi.AbstractServiceDefinitionLocator;
import com.neeve.toa.spi.ServiceDefinitionLocator;
import com.neeve.trace.Tracer;
import com.neeve.trace.Tracer.Level;
import com.neeve.util.UtlTime;

/**
 * The Market simulator/driver.
 * <p>
 * This class is a bare bones driver that responds to
 * {@link MarketNewOrderSingle}s from the EMS and records statistics.
 */
@AppVersion(1)
@AppHAPolicy(HAPolicy.EventSourcing)
final public class Market extends TopicOrientedApplication {
    private final Tracer tracer = RootConfig.ObjectConfig.createTracer(RootConfig.ObjectConfig.get("market"));
    // whether to use one or two buses.
    private final boolean useSingleBus = XRuntime.getValue("simulator.useSingleBus", false);
    private final Qos qos = Qos.valueOf(XRuntime.getValue("simulator.qos", "Guaranteed"));
    private final boolean sendFills = XRuntime.getValue("simulator.market.sendFills", true);
    private final boolean sendAcks = XRuntime.getValue("simulator.market.sendAcks", true);
    private final XString orderId = XString.create(32, true, true);
    private final XString lastSlice = XString.create(10, true, true);
    private int numSlices;
    private long lastRemaining = 0;
    private long lastRemainingChangeTs = 0;

    @AppStat
    private final Counter msgCount = StatsFactory.createCounterStat("Market Message Count");
    @AppStat
    private final Latencies tfsLatencies = StatsFactory.createLatencyStat("Time To First Slice");

    public Market() {
    }

    ///////////////////////////////////////////////////////////////////////////////
    // Runtime Configuration                                                     //
    //                                                                           //
    // Hornet's TopicOrientApplication class has many hooks to allow config      //
    // to be augmented programmatically at runtime.                              //
    ///////////////////////////////////////////////////////////////////////////////

    @Override
    final public void onEngineDescriptorInjected(final AepEngineDescriptor engineDescriptor) throws Exception {
        tracer.log("Engine Descriptor injected", Level.INFO);
        // allow starting market before ems is up ... will cause it to retry the bus connnection:
        engineDescriptor.setMessagingStartFailPolicy(AepEngine.MessagingStartFailPolicy.NeverFail);
        engineDescriptor.setMessageBusBindingFailPolicy(AepEngine.MessageBusBindingFailPolicy.Reconnect);
        if (useSingleBus) {
            _tracer.log("Removing bus 'market' for multi bus configuration", Tracer.Level.INFO);
            engineDescriptor.removeBus("market");
            engineDescriptor.clearBusManagerProperties("market");
        } else {
            _tracer.log("Removing bus 'ems' for multi bus configuration", Tracer.Level.INFO);
            engineDescriptor.removeBus("ems");
            engineDescriptor.clearBusManagerProperties("ems");
        }
    }

    /**
     * We use a custom service definition locator so that we can switch between
     * multiple and single bus configurations.
     * <p>
     * This is not typically necessary ... applications usually package their
     * service definitions on the classpath and use the
     * {@link DefaultServiceDefinitionLocator}.
     */
    @Override
    public ServiceDefinitionLocator getServiceDefinitionLocator() {
        return new ServiceLoader();
    }

    private final class ServiceLoader extends AbstractServiceDefinitionLocator {

        @Override
        public final void locateServices(final Set<URL> urls) throws Exception {
            if (useSingleBus) {
                urls.add(new File(XRuntime.getRootDirectory(), "conf/services/singlebus/marketService.xml").toURI().toURL());
            } else {
                urls.add(new File(XRuntime.getRootDirectory(), "conf/services/multibus/marketService.xml").toURI().toURL());
            }
        }
    }

    @Override
    public final Qos getChannelQos(final ToaService service, final ToaServiceChannel channel) {
        return qos;
    }

    ///////////////////////////////////////////////////////////////////////////////
    // EVENT & MESSAGE HANDLERS                                                  //
    //                                                                           //
    // Event handlers are called by the underlying applications AepEngine.       //
    //                                                                           //
    // NOTE: An Event Sourcing applicaton must be able to identically            //
    // recover its state and generate the *same* outbound messages via replay    //
    // of the its input events at a later time or on a different system.         //
    // Thus, for an application using Event Sourcing, it is crucial that the     //
    // app not make any changes to its state that are based on the local system  //
    // such as System.currentTimeMillis() or interacting with the file system.   //
    //                                                                           //
    // Event handlers are not called concurrently so synchronization is not      //
    // needed.                                                                   // 
    ///////////////////////////////////////////////////////////////////////////////

    /**
     * Handler for an order from the EMS.
     * <p>
     * This handler records statistics for orders processed and sends back order
     * acknowledgements and fills when configured to do so.
     * 
     * @param message
     *            The new order single.
     */
    @EventHandler
    final public void onNewOrderSingle(final MarketNewOrderSingle message) {
        final long now = UtlTime.now();
        msgCount.increment();

        // first slice for this order? If so capture first slice latencies.
        if (isFirstSlice(message)) {
            numSlices = 1;
            tfsLatencies.add(now - getOrderTs(message));
        } else {
            numSlices++;
        }

        sendMarketResponses(message, numSlices);
    }

    /**
     * Handler for WarmupComplete command which is forwarded from the Ems.
     * <p>
     * This handler and the {@link WarmupComplete} message perform cleanup
     * operations after warmup:
     * <ul>
     * <li>Resets statistics to purge data points from warmup from the test run.
     * <li>Triggers GC to flush any transient objects that might have been
     * promoted during warmup.
     * </ul>
     * 
     * @param message
     *            The warmup complete command
     */
    @EventHandler
    final public void onWarmupComplete(final com.neeve.tick2trade.messages.WarmupComplete message) {
        try {
            reset();
            gc();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ////////////////////////////////////////////////////////////////////////////
    // ULTILITY METHODS //
    // ////////////////////////////////////////////////////////////////////////////

    /**
     * Tests if the {@link MarketNewOrderSingle} represents the first slice for
     * an order.
     * 
     * @param message
     *            The order to test.
     * @return true if this is the first slice in an order.
     */
    final private boolean isFirstSlice(final MarketNewOrderSingle message) {
        orderId.clear();
        message.getClOrdIDTo(orderId);
        if (!lastSlice.isInitialized() || !lastSlice.equals(orderId)) {
            orderId.copyInto(lastSlice);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Extracts the order timestamp from the {@link MarketNewOrderSingle}. The
     * application tunnels the sending timestamp though the compliance id field.
     *
     * @param message
     *            The {@link MarketNewOrderSingle} from which to extract the
     *            timestamp.
     * @return the orginiating order timestamp.
     */
    final private long getOrderTs(final MarketNewOrderSingle message) {
        message.getComplianceIDField().getValueTo(orderId);
        return orderId.getValueAsLong();
    }

    final private void sendMarketResponse(final IRogMessage message) {
        try {
            sendMessage(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    final private void sendMarketResponses(final MarketNewOrderSingle message, final int sliceNumber) {
        final long now = System.currentTimeMillis();

        // send slice ack
        if (sendAcks) {
            sendMarketResponse(MarketOrderNewPopulator.populate(MarketOrderNew.create(), message, now));
        }

        // send fills
        if (sendFills) {
            sendMarketResponse(MarketTradePopulator.populate(MarketTrade.create(), message, now));
            if (sliceNumber % 2 == 0) {
                sendMarketResponse(MarketTradePopulator.populate(MarketTrade.create(), message, now));
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////////
    // COMMAND HANDLERS                                                          //
    //                                                                           //
    // Command handlers can be invoked remotely via management tools such as     //
    // Robin.                                                                    //
    // ////////////////////////////////////////////////////////////////////////////

    @Command(description = "Resets statistics for the application")
    final public void reset() throws Exception {
        orderId.clear();
        lastSlice.clear();
        //msgCount.reset();
        tfsLatencies.reset();

        if (getEngine() != null) {
            for (IAepBusManagerStats abms : getEngine().getStats().getBusManagerStats()) {
                abms.getLatencyManager().reset();
            }
            getEngine().getStats().getMessageTypeStatsTable().forEachPair(new IntObjectProcedure() {
                @Override
                public boolean apply(int key, Object value) {
                    ((IMessageTypeStats) value).reset();
                    return true;
                }
            });
        }
    }

    @Command(description = "Triggers a full gc")
    final public void gc() {
        Thread thread = new Thread() {
            public void run() {
                tracer.log("[Market] Triggering GC...", Level.INFO);
                System.gc();
            }
        };
        thread.start();
        return;
    }

    @Command(description = "Tests if the Market has received the given number of messages")
    final public boolean done(@Option(shortForm = 'c', longForm = "count", required = true, description = "The number of rcvd messages to check against.") final long count) throws Exception {
        final long remaining = count - msgCount.getCount();
        if (remaining == 0) {
            return true;
        } else {
            if (lastRemaining == remaining && ((System.currentTimeMillis() - lastRemainingChangeTs) >= 10000l)) {
                return true;
            } else {
                if (lastRemaining != remaining) {
                    lastRemaining = remaining;
                    lastRemainingChangeTs = System.currentTimeMillis();
                }
                return false;
            }
        }
    }
}
