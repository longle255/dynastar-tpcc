package ch.usi.dslab.lel.dynastar.tpcc;

import ch.usi.dslab.lel.dynastar.tpcc.benchmark.BenchContext;
import ch.usi.dslab.lel.dynastar.tpcc.tables.Row;
import ch.usi.dslab.lel.dynastar.tpcc.tables.Warehouse;
import ch.usi.dslab.lel.dynastar.tpcc.tpcc.TpccConfig;
import ch.usi.dslab.lel.dynastar.tpcc.tpcc.TpccUtil;
import ch.usi.dslab.lel.dynastarv2.Client;
import ch.usi.dslab.lel.dynastarv2.command.Command;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

public class TpccTerminal implements Runnable {
    private static final int SELECT_CUSTOMER_BY_NAME_PERCENTAGE = 0;
    private Logger logger;

    private Client clientProxy;
    private int terminalWarehouseID, terminalDistrictID, newOrderCounter;
    private int numTransactions, newOrderWeight, paymentWeight, deliveryWeight, orderStatusWeight, stockLevelWeight, limPerMin_Terminal;
    private Random gen;
    private TpccClient parent;
    private String terminalName;
    private int terminalId, warehouseCount;
    private boolean stopRunningSignal = false;

    private Semaphore sendPermits;
    private BenchContext.CallbackHandler callbackHandler;
    private boolean goodToGo = false;

    public TpccTerminal(Client clientProxy, String terminalName, int terminalId, int warehouseCount, int warehouseId, int districtId, int numTransactions, int newOrderWeight, int paymentWeight, int deliveryWeight,
                        int orderStatusWeight, int stockLevelWeight, int limPerMin_Terminal, TpccClient parent, BenchContext.CallbackHandler callbackHandler) {
        this.terminalId = terminalId;
        this.warehouseCount = warehouseCount;
        this.terminalWarehouseID = warehouseId;
        this.terminalDistrictID = districtId;
        this.newOrderCounter = 0;
        this.newOrderWeight = newOrderWeight;
        this.paymentWeight = paymentWeight;
        this.orderStatusWeight = orderStatusWeight;
        this.deliveryWeight = deliveryWeight;
        this.stockLevelWeight = stockLevelWeight;
        this.limPerMin_Terminal = limPerMin_Terminal;
        this.numTransactions = numTransactions;
        this.clientProxy = clientProxy;
        this.terminalName = terminalName;
        this.parent = parent;
        this.gen = new Random(System.nanoTime());
        this.callbackHandler = callbackHandler;
        sendPermits = new Semaphore(1);
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    public void run() {
        gen = new Random(System.nanoTime());
        executeTransactions(numTransactions);
        parent.signalTerminalEnded(this, newOrderCounter);
    }

    public void stopRunningWhenPossible() {
        stopRunningSignal = true;
        logger.debug("Terminal received stop signal!");
        logger.debug("Finishing current transaction before exit...");
    }

    public void doNewOrder(Consumer then) {
        int[] itemIDs, supplierWarehouseIds, orderQuantities;

        int districtID = TpccUtil.randomNumber(1, TpccConfig.configDistPerWhse, gen);
        int customerID = TpccUtil.getCustomerID(gen);

        int numItems = TpccUtil.randomNumber(5, 15, gen);
        itemIDs = new int[numItems];
        Set<ObjId> itemObjIds = new HashSet<>();
        supplierWarehouseIds = new int[numItems];
        Set<ObjId> supplierWarehouseObjIds = new HashSet<>();
        Set<ObjId> stockObjIds = new HashSet<>();
        orderQuantities = new int[numItems];
        int allLocal = 1;
        for (int i = 0; i < numItems; i++) {
            itemIDs[i] = TpccUtil.getItemID(gen);
            itemObjIds.add(new ObjId(Row.genSId("Item", itemIDs[i])));

            if (TpccUtil.randomNumber(1, 100, gen) > 1) {
                supplierWarehouseIds[i] = terminalWarehouseID;
            } else {
                do {
                    supplierWarehouseIds[i] = TpccUtil.randomNumber(1, this.warehouseCount, gen);
                }
                while (supplierWarehouseIds[i] == terminalWarehouseID && this.warehouseCount > 1);
                allLocal = 0;
            }
            supplierWarehouseObjIds.add(new ObjId(Row.genSId("Warehouse", supplierWarehouseIds[i])));
            stockObjIds.add(new ObjId(Row.genSId("Stock", supplierWarehouseIds[i], itemIDs[i])));
            orderQuantities[i] = TpccUtil.randomNumber(1, 10, gen);
        }

        // we need to cause 1% of the new orders to be rolled back.
        if (TpccUtil.randomNumber(1, 100, gen) == 1) {
            itemIDs[numItems - 1] = -12345;
            itemObjIds.add(new ObjId(Row.genSId("Item", itemIDs[numItems - 1])));
        }

        logger.debug("Starting txn: {}: {} (New-Order)");

        Map<String, Object> params = new HashMap<>();
        params.put("w_id", terminalWarehouseID);
        params.put("w_obj_id", new ObjId(Row.genSId("Warehouse", terminalWarehouseID)));
        params.put("d_id", districtID);
        params.put("d_obj_id", new ObjId(Row.genSId("District", terminalWarehouseID, districtID)));
        params.put("c_id", customerID);
        params.put("c_obj_id", new ObjId(Row.genSId("Customer", terminalWarehouseID, districtID, customerID)));
        params.put("ol_o_cnt", numItems);
        params.put("o_all_local", allLocal);
        params.put("itemIds", itemIDs);
        params.put("itemObjIds", itemObjIds);
        params.put("stockIds", stockObjIds);
        params.put("supplierWarehouseIds", supplierWarehouseIds);
        params.put("supplierWarehouseObjIds", supplierWarehouseObjIds);
        params.put("orderQuantities", orderQuantities);
        Command cmd = new Command(TpccCommandType.NEW_ORDER, params);
        clientProxy.executeCommand(cmd).thenAccept(then);
    }

    public void doPayment(Consumer then) {
        int districtID = TpccUtil.randomNumber(1, TpccConfig.configDistPerWhse, gen);

        int x = TpccUtil.randomNumber(1, 100, gen);
        int customerDistrictID;
        int customerWarehouseID;
        if (x <= 85) {
            customerDistrictID = districtID;
            customerWarehouseID = terminalWarehouseID;
        } else {
            customerDistrictID = TpccUtil.randomNumber(1, TpccConfig.configDistPerWhse, gen);
            do {
                customerWarehouseID = TpccUtil.randomNumber(1, this.warehouseCount, gen);
            }
            while (customerWarehouseID == terminalWarehouseID && this.warehouseCount > 1);
        }

        int y = TpccUtil.randomNumber(1, 100, gen);
        int customerID = -1;
        boolean customerByName;
        String customerLastName = null;
        if (y <= SELECT_CUSTOMER_BY_NAME_PERCENTAGE) {
            // 60% lookups by last name
            customerByName = true;
            customerLastName = TpccUtil.getLastName(new Random(System.nanoTime()));
        } else {
            // 40% lookups by customer ID
            customerByName = false;
            customerID = TpccUtil.getCustomerID(gen);
        }

        float paymentAmount = (float) (TpccUtil.randomNumber(100, 500000, gen) / 100.0);

        Map<String, Object> params = new HashMap<>();
        params.put("w_id", terminalWarehouseID);
        params.put("w_obj_id", new ObjId(Row.genSId("Warehouse", terminalWarehouseID)));
        params.put("d_id", districtID);
        params.put("d_obj_id", new ObjId(Row.genSId("District", terminalWarehouseID, districtID)));
        params.put("c_id", customerID);
        params.put("c_w_id", customerWarehouseID);
        params.put("c_w_obj_id", new ObjId(Row.genSId("Warehouse", customerWarehouseID)));
        params.put("c_d_id", customerDistrictID);
        params.put("c_d_obj_id", new ObjId(Row.genSId("District", customerWarehouseID, customerDistrictID)));
        params.put("c_obj_id", new ObjId(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerID)));
        params.put("c_by_name", customerByName);
        params.put("c_name", customerLastName);
        params.put("amount", paymentAmount);

        Command cmd = new Command(TpccCommandType.PAYMENT, params);
        clientProxy.executeCommand(cmd).thenAccept(then);
    }

    public void doOrderStatus(Consumer then) {
        int districtID = TpccUtil.randomNumber(1, TpccConfig.configDistPerWhse, gen);

        int y = TpccUtil.randomNumber(1, 100, gen);
        boolean customerByName = false;
        String customerLastName = null;
        int customerID = -1;
        if (y <= SELECT_CUSTOMER_BY_NAME_PERCENTAGE) {
            // 60% lookups by last name
            customerByName = true;
            customerLastName = TpccUtil.getLastName(new Random(System.nanoTime()));
        } else {
            // 40% lookups by customer ID
            customerByName = false;
            customerID = TpccUtil.getCustomerID(gen);
        }

        Map<String, Object> params = new HashMap<>();
        params.put("w_id", terminalWarehouseID);
        params.put("w_obj_id", new ObjId(Row.genSId("Warehouse", terminalWarehouseID)));
        params.put("d_id", districtID);
        ObjId districtObjId = new ObjId(Row.genSId("District", terminalWarehouseID, districtID));
        params.put("d_obj_id", districtObjId);
        params.put("c_id", customerID);
        params.put("c_obj_id", new ObjId(Row.genSId("Customer", terminalWarehouseID, districtID, customerID)));
        params.put("c_w_id", terminalWarehouseID);
        params.put("c_d_id", districtID);
        params.put("c_by_name", customerByName);
        params.put("c_name", customerLastName);

        Command cmd = new Command(TpccCommandType.ORDER_STATUS, params);
        int preferredPartitionId = clientProxy.getCache().getNode(districtObjId) != null ? clientProxy.getCache().getNode(districtObjId).getPartitionId() : -1;
        clientProxy.executeCommand(cmd, preferredPartitionId).thenAccept(then);
    }

    public void doDelivery(Consumer then) {
        int orderCarrierID = TpccUtil.randomNumber(1, 10, gen);
        Map<String, Object> params = new HashMap<>();
        params.put("w_id", terminalWarehouseID);
        params.put("w_obj_id", new ObjId(Row.genSId("Warehouse", terminalWarehouseID)));
        Set<ObjId> districtObjIds = new HashSet<>();
        for (int districtID = 1; districtID <= TpccConfig.configDistPerWhse; districtID++) {
            ObjId districtObjId = new ObjId(Row.genSId("District", terminalWarehouseID, districtID));
            districtObjId.includeDependencies = true;
            districtObjIds.add(districtObjId);
        }
        params.put("d_obj_ids", districtObjIds);
        params.put("o_carrier_id", orderCarrierID);
        Command cmd = new Command(TpccCommandType.DELIVERY, params);
        clientProxy.executeCommand(cmd).thenAccept(then);
    }


    public void doStockLevel(Consumer then) {
        int threshold = TpccUtil.randomNumber(10, 20, gen);
        int districtId = TpccUtil.randomNumber(1, TpccConfig.configDistPerWhse, gen);
        Map<String, Object> params = new HashMap<>();
        params.put("w_id", terminalWarehouseID);
        params.put("w_obj_id", new ObjId(Row.genSId("Warehouse", terminalWarehouseID)));
        params.put("d_id", districtId);
        ObjId districtObjId = new ObjId(Row.genSId("District", terminalWarehouseID, districtId));
        districtObjId.includeDependencies = true;
        params.put("d_obj_id", districtObjId);
        params.put("threshold", threshold);
        Set<ObjId> stockDistrictObjIds = TpccUtil.getStockDistrictId(terminalWarehouseID);
        stockDistrictObjIds.forEach(objId -> objId.includeDependencies = true);
        params.put("s_d_obj_ids", stockDistrictObjIds);
        Command cmd = new Command(TpccCommandType.STOCK_LEVEL, params);
        clientProxy.executeCommand(cmd).thenAccept(then);
    }

    void getPermit() {
        try {
            sendPermits.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void executeTransactions(int numTransactions) {

        boolean stopRunning = false;

        if (numTransactions != -1)
            logger.debug("Executing " + numTransactions + " transactions...");
        else
            logger.debug("Executing for a limited time...");

        for (int i = 0; (i < numTransactions || numTransactions == -1) && !stopRunning; i++) {
            getPermit();
            long transactionType = TpccUtil.randomNumber(1, 100, gen);
            int skippedDeliveries = 0, newOrder = 0;
            String transactionTypeName;

            long transactionStart = System.currentTimeMillis();
            if (transactionType <= newOrderWeight) {
//                System.out.println("Doing new order");
                transactionTypeName = "New-Order";
                BenchContext.CallbackContext context = new BenchContext.CallbackContext(this, TpccCommandType.NEW_ORDER);
                Consumer tmpAccept = o -> callbackHandler.accept(o, context);
                doNewOrder(tmpAccept);
                newOrderCounter++;
                newOrder = 1;
            } else if (transactionType <= newOrderWeight + stockLevelWeight && goodToGo) {
                transactionTypeName = "Stock-Level";
//                System.out.println("Doing "+transactionTypeName);
                BenchContext.CallbackContext context = new BenchContext.CallbackContext(this, TpccCommandType.STOCK_LEVEL);
                Consumer tmpAccept = o -> callbackHandler.accept(o, context);
                doStockLevel(tmpAccept);
            } else if (transactionType <= newOrderWeight + stockLevelWeight + orderStatusWeight  && goodToGo) {
                transactionTypeName = "Order-Status";
//                System.out.println("Doing "+transactionTypeName);
                BenchContext.CallbackContext context = new BenchContext.CallbackContext(this, TpccCommandType.ORDER_STATUS);
                Consumer tmpAccept = o -> callbackHandler.accept(o, context);
                doOrderStatus(tmpAccept);
            } else if (transactionType <= newOrderWeight + stockLevelWeight + orderStatusWeight + deliveryWeight && goodToGo) {
                BenchContext.CallbackContext context = new BenchContext.CallbackContext(this, TpccCommandType.DELIVERY);
                Consumer tmpAccept = o -> callbackHandler.accept(o, context);
                transactionTypeName = "Delivery";
//                System.out.println("Doing "+transactionTypeName);
                doDelivery(tmpAccept);
            } else {
                transactionTypeName = "Payment";
                BenchContext.CallbackContext context = new BenchContext.CallbackContext(this, TpccCommandType.PAYMENT);
                Consumer tmpAccept = o -> callbackHandler.accept(o, context);
                doPayment(tmpAccept);
            }

            long transactionEnd = System.currentTimeMillis();

            if (!transactionTypeName.equals("Delivery")) {
                parent.signalTerminalEndedTransaction(i, this.terminalName, transactionTypeName, transactionEnd - transactionStart, null, newOrder);
            } else {
                parent.signalTerminalEndedTransaction(i, this.terminalName, transactionTypeName, transactionEnd - transactionStart, (skippedDeliveries == 0 ? "None" : "" + skippedDeliveries + " delivery(ies) skipped."), newOrder);
            }

            if (limPerMin_Terminal > 0) {
                long elapse = transactionEnd - transactionStart;
                long timePerTx = 60000 / limPerMin_Terminal;

                if (elapse < timePerTx) {
                    try {
                        long sleepTime = timePerTx - elapse;
                        Thread.sleep((sleepTime));
                    } catch (Exception e) {
                    }
                }
            }


            if (stopRunningSignal) stopRunning = true;
        }
    }

    public void releasePermit() {
        sendPermits.release();
    }

    public void onCacheInvalidated() {
        this.goodToGo = true;
    }
}
