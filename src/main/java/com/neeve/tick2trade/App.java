package com.neeve.tick2trade;

import java.io.File;
import java.net.URL;
import java.util.Set;

import cern.colt.function.IntObjectProcedure;

import com.neeve.ci.*;
import com.neeve.cli.annotations.Command;
import com.neeve.rog.IRogMessage;
import com.neeve.root.*;
import com.neeve.toa.DefaultServiceDefinitionLocator;
import com.neeve.toa.TopicOrientedApplication;
import com.neeve.toa.service.ToaService;
import com.neeve.toa.service.ToaServiceChannel;
import com.neeve.toa.spi.AbstractServiceDefinitionLocator;
import com.neeve.toa.spi.ServiceDefinitionLocator;
import com.neeve.trace.*;
import com.neeve.trace.Tracer.Level;
import com.neeve.xbuf.*;
import com.neeve.aep.*;
import com.neeve.aep.AepEngine.HAPolicy;
import com.neeve.aep.annotations.*;
import com.neeve.aep.event.AepChannelDownEvent;
import com.neeve.aep.event.AepChannelUpEvent;
import com.neeve.aep.event.AepMessagingStartedEvent;
import com.neeve.server.app.annotations.*;
import com.neeve.sma.MessageBusBinding;
import com.neeve.sma.MessageChannel;
import com.neeve.sma.MessageChannel.Qos;
import com.neeve.sma.MessageLatencyManager.MessagingDirection;
import com.neeve.sma.MessageLatencyManager.UpdateListener;
import com.neeve.sma.MessageView;
import com.neeve.sma.SmaException;
import com.neeve.stats.IStats.Latencies;
import com.neeve.stats.StatsFactory;
import com.neeve.tick2trade.messages.*;

/**
 * The application entry point for the execution management system.
 * <p>
 * <h2>Overview</h2>
 * In all this application has 4 main components:
 * <ul>
 * <li>The EMS (execution management system)
 * <li>The SOR (smart order router)
 * <li>The Client (serves as a driver for this App simulating client orders)
 * <li>The Market (servers as a driver for this App by simulating an exchange)
 * </ul>
 * The EMS is responsible for tracking state related to new order requests from
 * clients and brokering the fulfillment of the order between the executing
 * trader (Client) and liquidity venues (Market).
 * <p>
 * Order routing is done by a Smart Order Router (SOR) which determines how a
 * particular order should be sliced between different liquidity venues for
 * fulfillment. In this application the SOR is embedded in the same process as
 * the {@link Ems} for the lowest possible latency, but it would also be
 * possible for the SOR to be located in another process.
 * <p>
 * The Client and Market apps in the driver package drive the trading flow and
 * allow capture of end to end stats. Because they capture end to end latency
 * they should be run on the same host.
 * <p>
 * <h2>Metrics of Interest</h2>
 * <ul>
 * <li><b>Tick To Trade (ttt)</b>: Tick To Trade measures the latency from when
 * the timestamp of the tick upon which the {@link Sor} makes a decision based
 * on market data to the time that the trade is sent on the wire to the
 * exchange.
 * <p>
 * This metric is important for trading algorithms in terms of indicating how
 * quickly the trading system to capitalize on an advertised price.
 * <p>
 * Because this application doesn't use real market data, TTT is approximated by
 * taking the time that the {@link Sor} creates its first slice to the time at
 * which the slice is written the wire (the time at which it is received by the
 * market can't be computed because the Ems and Market may be running on
 * different system which will have clock skew). A final note on TTT: the
 * platform does have plugins for market data providers that can provide
 * extremely low (e.g. single digit microsecond) latency from ticker plant to
 * process, but to maximize the portability of this application this has not
 * been incoroporated here.
 * <li><b>Time To First Slice (tfs)</b>: Measures the time from when the trader
 * decided to make a trade to the time at which the first slice from the
 * {@link Sor} made it to the exchange.
 * <p>
 * TFS, is important from the trader's perspective in terms of measuring how
 * quickly the EMS can route its order to an exchange.
 * <p>
 * In this application the TFS is measured in 2 ways: In process and End To End.
 * <p>
 * The In Process TFS is measured in this class as the time from when the packet
 * containing the client order is received by the EMS process to the time when
 * the first slice has been serialized and written to the market process.
 * <p>
 * The End to End TFS can only be reliably measured by collocating the client
 * and market applications on the same host and computing the timestamp based
 * off of a timestamp tunneled through the application in the
 * {@link EMSNewOrderSingle}s' and {@link MarketNewOrderSingle}s' compliance id
 * fields.
 * <p>
 * Between In Process and End To End TFS, In Process is a better measure of
 * application performance as it isn't skewed by network latency which can be
 * considered a constant.
 * </ul>
 * 
 * <h2>Mechanics</h1>
 * 
 */
@AppVersion(1)
@AppHAPolicy(HAPolicy.EventSourcing)
final public class App extends TopicOrientedApplication {
    final private Tracer tracer = RootConfig.ObjectConfig.createTracer(RootConfig.ObjectConfig.get("ems"));

    /**
     * Hooks into platform latency statistics to calculate TTFS and Tick To
     * Trade Latencies
     */
    final private class TickToTradeCalculator implements UpdateListener {
        @Override
        public void onUpdate(MessageBusBinding binding, MessageView view, MessagingDirection direction) {
            if (direction == MessagingDirection.Outbound && view instanceof MarketNewOrderSingle) {
                MarketNewOrderSingle mnos = (MarketNewOrderSingle) view;
                timeToFirstSliceLatencies.add(mnos.getPreWireTs() - mnos.getPostWireTs());
                tickToTradeLatencies.add(mnos.getPreWireTs() - mnos.getTickTs());
            }
        }
    }

    static {
        // Enable framing for latency critical message types:
        // When framing is enabled the field deserialization is
        // defered and done on demand by accessors. 
        EMSNewOrderSingle.setDesyncPolicy(XbufDesyncPolicy.valueOf(XRuntime.getValue("simulator.ems.emsnewordersingle.desyncpolicy", "FrameFields")));
        MarketOrderNew.setDesyncPolicy(XbufDesyncPolicy.valueOf(XRuntime.getValue("simulator.ems.marketordernew.desyncpolicy", "FrameFields")));
        MarketTrade.setDesyncPolicy(XbufDesyncPolicy.valueOf(XRuntime.getValue("simulator.ems.markettrade.desyncpolicy", "FrameFields")));
        System.out.println("******* Desync Policies *********");
        System.out.println("...EMSNewOrderSingle.." + EMSNewOrderSingle.getDesyncPolicy());
        System.out.println("...MarketOrderNew....." + MarketOrderNew.getDesyncPolicy());
        System.out.println("...MarketTrade........" + MarketTrade.getDesyncPolicy());
        System.out.println("******* Desync Policies *********");
    }

    // whether to use one or two buses
    final private boolean useSingleBus = XRuntime.getValue("simulator.useSingleBus", false);
    final private Ems ems = new Ems(this, tracer);
    final private Sor sor = new Sor(this, tracer);

    // stats:
    @AppStat
    final Latencies tickToTradeLatencies = StatsFactory.createLatencyStat("In Proc Tick To Trade");
    @AppStat
    final Latencies timeToFirstSliceLatencies = StatsFactory.createLatencyStat("In Proc Time To First Slice");
    final TickToTradeCalculator tickToTradeListener = new TickToTradeCalculator();

    private volatile MessageChannel mcontrol;

    ///////////////////////////////////////////////////////////////////////////////
    // Runtime Configuration                                                     //
    //                                                                           //
    // Hornet's TopicOrientApplication class has many hooks to allow config      //
    // to be augmented programmatically at runtime.                              //
    ///////////////////////////////////////////////////////////////////////////////

    @Override
    final public void onEngineDescriptorInjected(final AepEngineDescriptor engineDescriptor) throws Exception {
        engineDescriptor.setMessagingStartFailPolicy(AepEngine.MessagingStartFailPolicy.NeverFail);
        engineDescriptor.setMessageBusBindingFailPolicy(AepEngine.MessageBusBindingFailPolicy.Reconnect);
        if (useSingleBus) {
            engineDescriptor.removeBus("market");
            engineDescriptor.clearBusManagerProperties("market");
        }
    }

    /**
     * We use a custom service definition locator so that we can switch between
     * multiple and single bus configurations.
     * <p>
     * This is not typically necessary ... applications usually package their
     * service definitions on the classpath and use the
     * {@link DefaultServiceDefinitionLocator}. We do this here to allow
     * switching between the typical single bus scenario and more advanced
     * multiple bus scenario.
     */
    @Override
    public ServiceDefinitionLocator getServiceDefinitionLocator() {
        return new ServiceLoader();
    }

    private final class ServiceLoader extends AbstractServiceDefinitionLocator {

        @Override
        public void locateServices(Set<URL> urls) throws Exception {
            // TODO Auto-generated method stub
            if (useSingleBus) {
                urls.add(new File(XRuntime.getRootDirectory(), "conf/services/singlebus/marketService.xml").toURI().toURL());
                urls.add(new File(XRuntime.getRootDirectory(), "conf/services/singlebus/emsService.xml").toURI().toURL());
            } else {
                urls.add(new File(XRuntime.getRootDirectory(), "conf/services/multibus/marketService.xml").toURI().toURL());
                urls.add(new File(XRuntime.getRootDirectory(), "conf/services/multibus/emsService.xml").toURI().toURL());
            }
        }
    }

    @Override
    public Qos getChannelQos(ToaService service, ToaServiceChannel channel) {
        return Qos.BestEffort;
    }

    final protected void onAppInitialized() throws Exception {
        tracer.log("Parameters", Level.INFO);
        ems.configure();
        sor.configure();
        tracer.log("...singleBus=" + useSingleBus, Level.INFO);
    }

    final protected void addHandlerContainers(Set<Object> containers) {
        containers.add(ems);
        containers.add(sor);
    }

    final protected void addAppStatContainers(Set<Object> containers) {
        containers.add(ems);
    }

    @EventHandler(source = "mcontrol@market")
    public void onMarketControlChannelUp(AepChannelUpEvent event) {
        mcontrol = event.getMessageChannel();
    }

    @EventHandler(source = "mcontrol@market")
    public void onMarketControlChannelUp(AepChannelDownEvent event) {
        mcontrol = null;
    }

    /**
     * Send interceptor to dispatch SOR traffic in the current
     * transaction.
     * 
     * @param message
     *            The message.
     */
    final public void send(final IRogMessage message) {
        switch (message.getType()) {
        // for NewOrderSingle and Slice Commands
        // directly dispatch locally, rather than send:
        case MessageFactorySOR.ID_SORNewOrderSingle:
        case MessageFactoryEMS.ID_EMSSliceCommand:
            getEngine().getEventDispatcher().dispatchToEventHandlers(message);
            message.dispose();
            break;
        default:
            sendMessage(message);
            break;
        }
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

    @EventHandler
    public void onMessagingStarted(AepMessagingStartedEvent event) {
        if (getEngine() != null) {
            for (IAepBusManagerStats abms : getEngine().getStats().getBusManagerStats()) {
                if (abms.getLatencyManager() != null) {
                    abms.getLatencyManager().setUpdateListener(tickToTradeListener);
                }
            }
        }
    }

    /**
     * Handler for WarmupComplete command which is forwarded from the Client
     * when it has completed sending warmup messages.
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
        if (!isSingleBus()) {
            if (mcontrol != null) {
                try {
                    mcontrol.sendMessage(WarmupComplete.create(), null, 0);
                } catch (SmaException e) {
                    tracer.log("Failed to send warmup complete command to Market", Tracer.Level.WARNING);
                }
            } else {
                sendMessage(WarmupComplete.create());
            }
        }
        try {
            reset();
            gc();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    ///////////////////////////////////////////////////////////////////////////////
    // COMMAND HANDLERS                                                          //
    //                                                                           //
    // Command handlers can be invoked remotely via management tools such as     //
    // Robin.                                                                    //
    // ////////////////////////////////////////////////////////////////////////////

    @Command(description = "Resets statistics for the ems")
    final public void reset() throws Exception {
        ems.reset();
        sor.reset();
        tickToTradeLatencies.reset();
        timeToFirstSliceLatencies.reset();
        for (IAepBusManagerStats abms : getEngine().getStats().getBusManagerStats()) {
            if (abms.getLatencyManager() != null) {
                abms.getLatencyManager().reset();
            }
        }
        getEngine().getStats().getMessageTypeStatsTable().forEachPair(new IntObjectProcedure() {
            @Override
            public boolean apply(int key, Object value) {
                ((IAepEngineStats.IMessageTypeStats) value).reset();
                return true;
            }
        });
    }

    @Command(description = "Triggers a full gc")
    final public void gc() throws Exception {
        Thread thread = new Thread() {
            public void run() {
                tracer.log("[Ems] Triggering GC...", Tracer.Level.INFO);
                System.gc();
            }
        };
        thread.start();
    }

    final Ems getEms() {
        return ems;
    }

    final boolean isSingleBus() {
        return useSingleBus;
    }
}
