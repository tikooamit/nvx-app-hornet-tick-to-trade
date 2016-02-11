package com.neeve.tick2trade;

import com.neeve.ci.*;
import com.neeve.cli.annotations.Command;
import com.neeve.cli.annotations.Option;
import com.neeve.util.*;
import com.neeve.trace.*;
import com.neeve.lang.*;
import com.neeve.aep.annotations.*;
import com.neeve.server.app.annotations.*;
import com.neeve.tick2trade.acl.*;
import com.neeve.tick2trade.domain.*;
import com.neeve.tick2trade.driver.Market;
import com.neeve.tick2trade.messages.*;

/**
 * The Execution Management System (EMS).
 * <p>
 * <h2>Overview</h2>
 * The EMS is responsible for tracking state related to new order requests from
 * clients and brokering the fulfillment of the order between the executing
 * trader (Client) and liquidity venues (Market).
 * 
 * @see App
 */
final public class Ems {
    public enum OutboundMessage {
        SORNewOrderSingle, MarketNewOrderSingle, OrderNew, OrderEvent, Trade;
    }

    // order state
    private final XString orderIdTmp = XString.create(12, true, true);
    private final int orderPreallocateCount = XRuntime.getValue("simulator.ems.orderPreallocateCount", 1048576);
    private UtlPool<Order> orderPool = null;
    private XLinkedHashMap<XString, Order> orders = new XLinkedHashMap<XString, Order>(orderPreallocateCount);

    // stats
    @AppStat(name = "EMS Orders Received")
    volatile long rcvdOrderCount;
    @AppStat(name = "EMS Messages Received")
    volatile long rcvdMessageCount;

    // others
    private final App app;

    // command state
    private long lastRemaining;
    private long lastRemainingChangeTs;

    Ems(final App app, final Tracer tracer) {
        this.app = app;
    }

    final void configure() throws Exception {
        // pe-allocate order states
        orderPool = Order.createPool(orderPreallocateCount);

        // pre-allocate strategy params
        System.out.println("Preallocating strategy params with " + orderPool.size() + " entries");
        com.neeve.tick2trade.messages.StrategyParams params = com.neeve.tick2trade.messages.StrategyParams.create();
        UtlPool<com.neeve.tick2trade.messages.StrategyParams> pool = params.getPool();
        params.dispose();
        for (int i = 0; i < orderPreallocateCount - 1; i++) {
            params = new com.neeve.tick2trade.messages.StrategyParams();
            params.setPool(pool);
            pool.put(params);
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

    /**
     * Handler for {@link EMSNewOrderSingle} messages sent by the client.
     * <p>
     * NewOrderSingles are dispatched to the SOR which will route the order to a
     * liquidity venue. In this simple sample we only have one
     * 
     * @param message
     *            The new order from a client.
     */
    @EventHandler
    final public void onNewOrderSingle(final EMSNewOrderSingle message) {
        rcvdOrderCount++;
        rcvdMessageCount++;
        final Order order = EMSNewOrderSingleExtractor.extract(message, orderPool.get(null));
        order.setNosPostWireTs(message.getPostWireTs());
        orders.put(order.getClOrdId(), order);
        // dispatch a SORNewOrderSingle to the SOR for market routing.
        app.send(SORNewOrderSinglePopulator.populate(SORNewOrderSingle.create(), order));
        // issue an EMSOrderNew which serves as an acknowledgement to the
        // issuing client.
        app.send(EMSOrderNewPopulator.populate(EMSOrderNew.create(), order));
    }

    /**
     * Handler for {@link EMSSliceCommand} messages issued by the {@link Sor}.
     * <p>
     * The slice command causes a {@link MarketNewOrderSingle} to be sent to the
     * market.
     * 
     * @param message
     *            The new order from the {@link Sor}.
     */
    @EventHandler
    final public void onSliceCommand(final EMSSliceCommand message) {
        rcvdMessageCount++;
        message.getOrderIDTo(orderIdTmp);
        final Order order = orders.get(orderIdTmp);
        final MarketNewOrderSingle marketNewOrderSingle = MarketNewOrderSinglePopulator.populate(MarketNewOrderSingle.create(), order);
        if (order.getNosPostWireTs() > 0l) {
            marketNewOrderSingle.setPostWireTs(order.getNosPostWireTs());
            order.setNosPostWireTs(0l);
        }
        marketNewOrderSingle.setTickTs(message.getTickTs());
        app.send(marketNewOrderSingle);
    }

    /**
     * Handler for {@link MarketTrade} issued by a liquidity venue when a trade
     * is completed.
     * <p>
     * This handler sends a {@link EMSTrade} back to the initiating client.
     * 
     * @param message
     *            The market trade from the {@link Market}.
     */
    @EventHandler
    final public void onMarketTrade(final MarketTrade message) {
        rcvdMessageCount++;
        orderIdTmp.clear();
        message.getOrderIDTo(orderIdTmp);
        app.send(EMSTradePopulator.populate(EMSTrade.create(), orders.get(orderIdTmp)));
    }

    /**
     * Handler for {@link MarketOrderNew} issued by a liquidity venue when a
     * trade is accepted.
     * 
     * @param message
     *            The order new from {@link Market}.
     */
    @EventHandler
    final public void onMarketOrderNew(final MarketOrderNew message) {
        rcvdMessageCount++;
    }

    ///////////////////////////////////////////////////////////////////////////////
    // COMMAND HANDLERS                                                          //
    //                                                                           //
    // Command handlers can be invoked remotely via management tools such as     //
    // Robin.                                                                    //
    ///////////////////////////////////////////////////////////////////////////////

    final public void reset() throws Exception {
        rcvdOrderCount = 0;
        rcvdMessageCount = 0;
    }

    @Command(description = "Tests if the ems has received the given number of messages")
    final public boolean done(@Option(shortForm = 'c', longForm = "count", required = true, description = "The number of rcvd messages to check against.") final long count) throws Exception {
        final long remaining = count - rcvdOrderCount;
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

    @Command(description = "Gets the number of new order singles received")
    final public long getRcvdNosCount() throws Exception {
        return rcvdOrderCount;
    }
}
