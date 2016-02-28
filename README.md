# nvx-app-hornet-tick-to-trade
A Hornet Based EMS App For Low Latency Trading

###  SPEED
**Time to First Slice**: How long from the time a trader makes trading decision to the time when the market reflects that decision.

**Tick to Trade**: “How fast can your EMS respond to advertised price?”

### EASE
“How agile are you at being able to change your business logic?”

**See how an application on top of the Hornet and The X Platform a 100% Pure Jave application platform with no infrastructure code needed can serve up trades in the 10s of microsends.**

![Tick To Trade App Flow](/docs/flow-diagram.png)

```java
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
        // update statistics:
        rcvdOrderCount++;
        rcvdMessageCount++;
        
        // read fields from EMSNewOrderSingle (pojo) into an Order object
        final Order order = EMSNewOrderSingleExtractor.extract(message, orderPool.get(null));
        order.setNosPostWireTs(message.getPostWireTs());
        orders.put(order.getClOrdId(), order);
        
        // dispatch a SORNewOrderSingle to the SOR for market routing.
        app.send(SORNewOrderSinglePopulator.populate(SORNewOrderSingle.create(), order));
        
        // issue an EMSOrderNew which serves as an acknowledgement to the
        // issuing client.
        app.send(EMSOrderNewPopulator.populate(EMSOrderNew.create(), order));
    }
```

As Reference:
* See the [Getting Started Page](https://github.com/neeveresearch/nvx-app-hornet-tick-to-trade/wiki/Getting-Started) for this application.
* [Take a look at the presentation](http://docs.neeveresearch.com/decks/nvx-low-latency-apps.pdf)
* [Introduction to the X Platform](http://www.neeveresearch.com/introduction)

---

_This is an early access preview. To build and run this application on your own you will need an X Platform evaluation license for the 3.1 Release which is due out immenently. Instructions will be added here upon release. For now, hang in there and just have a look through the code._ 
