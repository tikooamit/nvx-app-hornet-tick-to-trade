package com.neeve.tick2trade.driver;

import java.io.File;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Set;

import com.google.protobuf.CodedOutputStream;
import com.neeve.aep.AepBusManager;
import com.neeve.aep.AepEngine;
import com.neeve.aep.AepEngine.HAPolicy;
import com.neeve.aep.AepEngineDescriptor;
import com.neeve.aep.annotations.EventHandler;
import com.neeve.aep.event.AepChannelUpEvent;
import com.neeve.ci.XRuntime;
import com.neeve.cli.annotations.Command;
import com.neeve.io.IOElasticBuffer;
import com.neeve.lang.XString;
import com.neeve.pkt.PktHeader;
import com.neeve.pkt.PktPacket;
import com.neeve.pkt.PktSerializer;
import com.neeve.root.RootConfig;
import com.neeve.server.app.annotations.AppHAPolicy;
import com.neeve.server.app.annotations.AppMain;
import com.neeve.server.app.annotations.AppVersion;
import com.neeve.sma.MessageChannel;
import com.neeve.sma.MessageChannel.Qos;
import com.neeve.sma.SmaException;
import com.neeve.tick2trade.acl.EMSNewOrderSinglePopulator;
import com.neeve.tick2trade.messages.EMSNewOrderSingle;
import com.neeve.tick2trade.messages.MessageFactoryEMS;
import com.neeve.tick2trade.messages.WarmupComplete;
import com.neeve.toa.DefaultServiceDefinitionLocator;
import com.neeve.toa.TopicOrientedApplication;
import com.neeve.toa.service.ToaService;
import com.neeve.toa.service.ToaServiceChannel;
import com.neeve.toa.spi.AbstractServiceDefinitionLocator;
import com.neeve.toa.spi.ServiceDefinitionLocator;
import com.neeve.trace.Tracer;
import com.neeve.trace.Tracer.Level;
import com.neeve.util.UtlThread;
import com.neeve.util.UtlTime;

@AppVersion(1)
@AppHAPolicy(HAPolicy.EventSourcing)
final public class Client extends TopicOrientedApplication {
    final public class Runner extends Thread {
        final private int count;
        final private int rate;
        final private long affinity;
        final private long statsIntervalInMillis;
        boolean running;
        long recvCount;
        long deltaRecvCount;
        long firstMsgTs;
        long deltaMsgTs;

        public Runner(final int count, final int rate, final long affinity, final long statsIntervalInMillis) {
            super("Client Runner");
            this.count = count;
            this.rate = rate;
            this.affinity = affinity;
            this.statsIntervalInMillis = statsIntervalInMillis;
        }

        final private int syncVarint64(long value, final ByteBuffer buffer, final int offset) {
            int len = 0;
            while (true) {
                if ((value & ~0x7F) == 0) {
                    buffer.put(offset + len, (byte) value);
                    return ++len;
                } else {
                    buffer.put(offset + len, (byte) (((int) value & 0x7F) | 0x80));
                    value >>>= 7;
                    len++;
                }
            }
        }

        /**
         * Constructs a preserialized new order single for a template buffer.
         * <p>
         * We do this so as not to add any unecessary overhead in the TFS
         * calculation.
         * 
         * @return A 'raw' serialized new order single
         */
        final private ByteBuffer serializedNewOrderSingle() {
            // orderId
            orderId++;
            orderIdStr.clear();
            orderIdStr.setValue(orderId);
            if (orderIdStr.length() != 7) {
                throw new RuntimeException("ClOrdId length has changed (original=7, now=" + orderIdStr.length() + ")");
            }
            orderIdStr.copyInto(buffer, PktHeader.STATIC_HEADER_LENGTH + orderIdOffset);

            // send and transact time
            final long nowMS = System.currentTimeMillis();
            syncVarint64(nowMS, buffer, PktHeader.STATIC_HEADER_LENGTH + transactTimeOffset);
            syncVarint64(nowMS, buffer, PktHeader.STATIC_HEADER_LENGTH + sendingTimeOffset);

            // orderTs
            final long now = UtlTime.now();
            if (orderTsLength != CodedOutputStream.computeRawVarint64Size(now)) {
                throw new RuntimeException("order ts length has changed (original=" + orderTsLength + ", now=" + CodedOutputStream.computeRawVarint64Size(now) + ")");
            }
            syncVarint64(now, buffer, PktHeader.STATIC_HEADER_LENGTH + orderTsOffset);

            // compliance id
            complianceIdStr.clear();
            complianceIdStr.setValue(now);
            if (complianceIdStr.length() != complianceIdLength) {
                throw new RuntimeException("ComplianceId length has changed (original=" + complianceIdLength + ", now=" + complianceIdStr.length() + ")");
            }
            complianceIdStr.copyInto(buffer, PktHeader.STATIC_HEADER_LENGTH + complianceIdOffset);

            // position to start of serialized container and return
            return (ByteBuffer) buffer.position(PktHeader.STATIC_HEADER_LENGTH);
        }

        final public void onMessageReceipt() {
            deltaRecvCount++;
            recvCount++;
            if (firstMsgTs == 0) {
                firstMsgTs = deltaMsgTs = UtlTime.now() / 1000;
            }
        }

        final public void abort() {
            running = false;
        }

        final public boolean running() {
            return running;
        }

        final public void running(boolean val) {
            running = val;
        }

        @Override
        final public void start() {
            running = true;
            super.start();
        }

        @Override
        final public void run() {
            UtlThread.setCPUAffinityMask(affinity);
            try {
                // send NOS
                System.out.println("Sending...");
                int i = 0;
                final long start = System.nanoTime();
                long istart = start;
                int di = 0;
                final long statInterval = statsIntervalInMillis * 1000000;
                final long nanosPerMsg = rate > 0 ? (1000000000l / rate) : 0;
                final boolean statsEnabled = statsIntervalInMillis > 0;
                long next = start + nanosPerMsg;
                while (i < count && running) {
                    final long current = System.nanoTime();
                    if (current >= next) {
                        send(MessageFactoryEMS.VFID, MessageFactoryEMS.ID_EMSNewOrderSingle, serializedNewOrderSingle());
                        next += nanosPerMsg;
                        di++;
                        i++;
                    }

                    if (statsEnabled && current - istart > statInterval) {
                        // send
                        int deltaSendRate = (int) ((di * 1000000000L) / (current - istart));
                        int overallSendRate = (int) ((i * 1000000000L) / (current - start));
                        tracer.log("[Client] Sent=" + i + " DRate=" + deltaSendRate + " Rate=" + overallSendRate, Tracer.Level.INFO);
                        istart = current;
                        di = 0;

                        // recv
                        final long ts = System.currentTimeMillis();
                        final long lastCount = recvCount;
                        final long lastDeltaRecvCount = deltaRecvCount;
                        final long lastDeltaMsgTs = deltaMsgTs;
                        deltaMsgTs = ts;
                        deltaRecvCount = 0;
                        int deltaRecvRate = (int) ((lastDeltaRecvCount * 1000l) / (ts - lastDeltaMsgTs));
                        int overallRecvRate = 0;
                        if (ts > firstMsgTs) {
                            overallRecvRate = (int) ((lastCount * 1000l) / (ts - firstMsgTs));
                        }
                        tracer.log("[Client] Rcvd=" + lastCount + " DRate=" + deltaRecvRate + " Rate=" + overallRecvRate, Tracer.Level.VERBOSE);
                    }
                }
                flush();
                final long current = System.nanoTime();
                final int overallSendRate = (int) ((i * 1000000000L) / (current - start));
                final int overallRecvRate = (int) ((recvCount * 1000000000L) / (current - start));
                tracer.log("[Client] Done (Sent=" + i + ", Rate=" + overallSendRate + ") (Recvd=" + recvCount + ", Rate=" + overallRecvRate + ")", Tracer.Level.VERBOSE);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                running = false;
            }
        }
    }

    final private static Qos qos = Qos.valueOf(XRuntime.getValue("simulator.qos", "Guaranteed"));
    final public static long senderAffinity = UtlThread.parseAffinityMask(XRuntime.getValue("simulator.client.sendAffinity", "0"));
    final public static long clientStatsInterval = XRuntime.getValue("simulator.client.statsIntervalMillis", 5000);
    final public static long manualWarmupIntermission = XRuntime.getValue("simulator.manualWarmupIntermission", 30000);
    final public static int manualWarmupCount = XRuntime.getValue("simulator.manualWarmupCount", 0);
    final public static int manualWarmupRate = XRuntime.getValue("simulator.manualWarmupRate", 10000);
    final public static int manualSendCount = XRuntime.getValue("simulator.manualSendCount", 0);
    final public static int manualSendRate = XRuntime.getValue("simulator.manualSendRate", 1000);
    final public static boolean manualRun = XRuntime.getValue("simulator.manualRun", false);

    final private static ByteBuffer buffer;
    final private static XString orderIdStr = XString.create(12, true, true);
    final private static int orderTsOffset;
    final private static int orderTsLength;
    final private static int orderIdOffset;
    final private static int complianceIdOffset;
    final private static int complianceIdLength;
    final private static int sendingTimeOffset;
    final private static int transactTimeOffset;
    final private static XString complianceIdStr = XString.create(12, true, true);
    private static int orderId = 1000000;

    // private members
    final private static Tracer tracer = RootConfig.ObjectConfig.createTracer(RootConfig.ObjectConfig.get("client"));
    private Runner _runner;
    private MessageChannel ordersChannel;

    static {
        // id generator
        orderIdStr.setValue(orderId);

        // prepare message template (in serialized form)
        final EMSNewOrderSingle nos = EMSNewOrderSinglePopulator.populate(EMSNewOrderSingle.create(), orderIdStr, new java.util.Random(System.currentTimeMillis()));
        final PktPacket packet = nos.serializeToPacket();
        final IOElasticBuffer ebuffer = IOElasticBuffer.create(null, 1, true);
        ebuffer.setLength(packet.getSerializedLength());
        ebuffer.getIOBuffer().getBuffer().mark();
        PktSerializer.create().serialize(packet, ebuffer.getIOBuffer(), null);
        ebuffer.getIOBuffer().getBuffer().reset();
        buffer = ebuffer.getIOBuffer().getBuffer();

        // find order ts, order id and compliance id offset
        orderTsLength = CodedOutputStream.computeRawVarint64Size(nos.getOrderTs());
        orderTsOffset = nos.getOrderTsField().getContentOffset();
        orderIdOffset = nos.getClOrdIDField().getContentOffset();
        complianceIdOffset = nos.getComplianceIDField().getContentOffset();
        complianceIdLength = nos.getComplianceIDField().getContentLength();
        sendingTimeOffset = nos.getSendingTimeField().getContentOffset();
        transactTimeOffset = nos.getTransactTimeField().getContentOffset();
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
        _tracer.log("Removing the default client bus...", Tracer.Level.INFO);
        engineDescriptor.removeBus("client");
        engineDescriptor.clearBusManagerProperties("client");
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
        public void locateServices(Set<URL> urls) throws Exception {
            urls.add(new File(XRuntime.getRootDirectory(), "conf/services/singlebus/emsService.xml").toURI().toURL());
        }
    }

    @Override
    public final Qos getChannelQos(final ToaService service, final ToaServiceChannel channel) {
        return qos;
    }

    final private void send(final int count, final int rate) throws Exception {
        System.out.println("Parameters");
        System.out.println("...count=" + count);
        System.out.println("...rate=" + rate);
        synchronized (this) {
            while (ordersChannel == null) {
                System.out.println("Waiting for orders channel to come up...");
                wait();
            }
        }
        _runner = new Runner(count < 0 ? Integer.MAX_VALUE : count, rate, senderAffinity, clientStatsInterval);
        _runner.running(true);
        _runner.start();
    }

    /**
     * Raw sending method.
     * <p>
     * Don't try this at home.
     * 
     * @param factoryId
     *            The factory id of the message
     * @param type
     *            The type id of the message
     * @param packet
     *            The serialized packet.
     * @throws Exception
     *             If there is an error sending.
     */
    final private void send(final short factoryId, final short type, final Object packet) throws Exception {
        if (packet instanceof ByteBuffer) {
            switch (factoryId) {
            case MessageFactoryEMS.VFID:
                sendMessage(EMSNewOrderSingle.createFromSerializedXbufContainer((ByteBuffer) packet));
                break;

            default:
                throw new IllegalArgumentException("Unknown message type in sent: factory: " + factoryId + ", type: " + type);
            }
        } else {
            switch (factoryId) {
            case MessageFactoryEMS.VFID:
                sendMessage(EMSNewOrderSingle.createFromXbufContainerPacket((PktPacket) packet));
                break;

            default:
                throw new IllegalArgumentException("Unknown message type in sent: factory: " + factoryId + ", type: " + type);
            }
        }
    }

    /**
     * Flushes all message bus binding.
     * <p>
     * This method allows flushing messages that have been sent during warmup to
     * ensure that they are reflected in post warmup results.
     */
    final private void flush() {
        for (AepBusManager busManager : getEngine().getBusManagers()) {
            try {
                busManager.getBusBinding().flush(null);
            } catch (SmaException e) {
                e.printStackTrace();
            }
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
    final public void onOrderNew(final com.neeve.tick2trade.messages.EMSOrderNew message) {
        if (_runner != null) {
            _runner.onMessageReceipt();
        }
    }

    @EventHandler
    final public void onTrade(final com.neeve.tick2trade.messages.EMSTrade message) {
        if (_runner != null) {
            _runner.onMessageReceipt();
        }
    }

    @EventHandler(source = "orders@ems")
    final public void onOrdersChannelUp(AepChannelUpEvent event) {
        synchronized (this) {
            //Listen
            ordersChannel = event.getMessageChannel();
            notifyAll();
        }

    }

    @Command(description = "Triggers a full GC.")
    final private void gc() throws Exception {
        Thread thread = new Thread() {
            public void run() {
                tracer.log("[Client] Triggering GC...", Level.INFO);
                System.gc();
            }
        };
        thread.start();
        return;
    }

    @AppMain
    final public void appMain(String[] args) throws Exception {
        UtlThread.setDefaultCPUAffinityMask();
        if (!manualRun) {
            return;
        }

        tracer.log("[Client] ...manualRun=" + manualRun, Level.INFO);
        tracer.log("[Client] ...manualWarmupCount=" + manualWarmupCount, Level.INFO);
        tracer.log("[Client] ...manualWarmupRate=" + manualWarmupRate, Level.INFO);
        tracer.log("[Client] ...manualSendCount=" + manualSendCount, Level.INFO);
        tracer.log("[Client] ...manualSendRate=" + manualSendRate, Level.INFO);
        tracer.log("[Client] ...manualSendAffinity=" + XRuntime.getValue("simulator.client.sendAffinity", "0"), Level.INFO);

        // warm up
        tracer.log("[Client] BEGINING WARMUP", Tracer.Level.INFO);
        send(manualWarmupCount, manualWarmupRate);
        _runner.join();
        tracer.log("[Client] WARMUP COMPLETE", Tracer.Level.INFO);

        // send warmup complete
        final WarmupComplete msg = WarmupComplete.create();
        sendMessage(msg);
        flush();

        // trigger GC
        gc();
        tracer.log("[Client] Sleeping for " + (manualWarmupIntermission / 1000) + "s to allow other agent to perform GC", Level.INFO);
        Thread.sleep(manualWarmupIntermission);
        java.lang.Compiler.disable();

        // do test
        send(manualSendCount, manualSendRate);
        _runner.join();
    }

    final public void onAppFinalized() {
        if (_runner != null) {
            _runner.abort();
        }
    }
}
