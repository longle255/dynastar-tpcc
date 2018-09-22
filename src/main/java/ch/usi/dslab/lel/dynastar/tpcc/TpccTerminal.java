package ch.usi.dslab.lel.dynastar.tpcc;

import ch.usi.dslab.lel.dynastar.tpcc.benchmark.BenchContext;
import ch.usi.dslab.lel.dynastar.tpcc.tpcc.TpccConfig;
import ch.usi.dslab.lel.dynastar.tpcc.tpcc.TpccUtil;
import ch.usi.dslab.lel.dynastarv2.Client;
import ch.usi.dslab.lel.dynastarv2.command.Command;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

public class TpccTerminal implements Runnable {
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
        int[] itemIDs, supplierWarehouseIDs, orderQuantities;

        int districtID = TpccUtil.randomNumber(1, TpccConfig.configDistPerWhse, gen);
        int customerID = TpccUtil.getCustomerID(gen);

        int numItems = TpccUtil.randomNumber(5, 15, gen);
        itemIDs = new int[numItems];
        supplierWarehouseIDs = new int[numItems];
        orderQuantities = new int[numItems];
        int allLocal = 1;
        for (int i = 0; i < numItems; i++) {
            itemIDs[i] = TpccUtil.getItemID(gen);
            if (TpccUtil.randomNumber(1, 100, gen) > 1) {
                supplierWarehouseIDs[i] = terminalWarehouseID;
            } else {
                do {
                    supplierWarehouseIDs[i] = TpccUtil.randomNumber(1, this.warehouseCount, gen);
                }
                while (supplierWarehouseIDs[i] == terminalWarehouseID && this.warehouseCount > 1);
                allLocal = 0;
            }
            orderQuantities[i] = TpccUtil.randomNumber(1, 10, gen);
        }

        // we need to cause 1% of the new orders to be rolled back.
        if (TpccUtil.randomNumber(1, 100, gen) == 1)
            itemIDs[numItems - 1] = -12345;

        logger.debug("Starting txn: {}: {} (New-Order)");

        Map<String, Object> params = new HashMap<>();
        params.put("w_id", terminalWarehouseID);
        params.put("d_id", districtID);
        params.put("c_id", customerID);
        params.put("ol_o_cnt", numItems);
        params.put("o_all_local", allLocal);
        params.put("itemIds", itemIDs);
        params.put("supplierWarehouseIDs", supplierWarehouseIDs);
        params.put("orderQuantities", orderQuantities);
        Command cmd = new Command(TpccCommandType.NEW_ORDER, params);
        cmd.setServerDecidesMove(true);
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
        if (y <= 60) {
            // 60% lookups by last name
            customerByName = true;
        } else {
            // 40% lookups by customer ID
            customerByName = false;
            customerID = TpccUtil.getCustomerID(gen);
        }

        float paymentAmount = (float) (TpccUtil.randomNumber(100, 500000, gen) / 100.0);

        Map<String, Object> params = new HashMap<>();
        params.put("w_id", terminalWarehouseID);
        params.put("d_id", districtID);
        params.put("c_id", customerID);
        params.put("c_w_id", customerWarehouseID);
        params.put("c_d_id", customerDistrictID);
        params.put("c_by_name", customerByName);
        params.put("amount", paymentAmount);
        params.put("randomSeed", System.nanoTime());


        Command cmd = new Command(TpccCommandType.PAYMENT, params);
        cmd.setServerDecidesMove(true);
        clientProxy.executeCommand(cmd).thenAccept(then);
    }

    public void doOrderStatus(Consumer then) {
        int districtID = TpccUtil.randomNumber(1, TpccConfig.configDistPerWhse, gen);

        int y = TpccUtil.randomNumber(1, 100, gen);
        boolean customerByName = false;
        int customerID = -1;
        if (y <= 60) {
            // 60% lookups by last name
            customerByName = true;
        } else {
            // 40% lookups by customer ID
            customerByName = false;
            customerID = TpccUtil.getCustomerID(gen);
        }

        Map<String, Object> params = new HashMap<>();
        params.put("w_id", terminalWarehouseID);
        params.put("d_id", districtID);
        params.put("c_id", customerID);
        params.put("c_by_name", customerByName);
        params.put("randomSeed", System.nanoTime());

        Command cmd = new Command(TpccCommandType.ORDER_STATUS, params);
        cmd.setServerDecidesMove(true);
        clientProxy.executeCommand(cmd).thenAccept(then);
    }

    public void doDelivery(Consumer then) {
        int orderCarrierID = TpccUtil.randomNumber(1, 10, gen);
        Map<String, Object> params = new HashMap<>();
        params.put("w_id", terminalWarehouseID);
        params.put("o_carrier_id", orderCarrierID);
        Command cmd = new Command(TpccCommandType.DELIVERY, params);
        cmd.setServerDecidesMove(true);
        clientProxy.executeCommand(cmd).thenAccept(then);
    }


    public void doStockLevel(Consumer then) {
        int threshold = TpccUtil.randomNumber(10, 20, gen);
        int districtId = TpccUtil.randomNumber(1, TpccConfig.configDistPerWhse, gen);
        Map<String, Object> params = new HashMap<>();
        params.put("w_id", terminalWarehouseID);
        params.put("d_id", districtId);
        params.put("threshold", threshold);
        Command cmd = new Command(TpccCommandType.STOCK_LEVEL, params);
        cmd.setServerDecidesMove(true);
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
            if (transactionType <= paymentWeight) {
                transactionTypeName = "Payment";
                BenchContext.CallbackContext context = new BenchContext.CallbackContext(this, TpccCommandType.PAYMENT);
                Consumer tmpAccept = o -> callbackHandler.accept(o, context);
                doPayment(tmpAccept);
            } else if (transactionType <= paymentWeight + stockLevelWeight) {
                transactionTypeName = "Stock-Level";
                BenchContext.CallbackContext context = new BenchContext.CallbackContext(this, TpccCommandType.STOCK_LEVEL);
                Consumer tmpAccept = o -> callbackHandler.accept(o, context);
                doStockLevel(tmpAccept);
            } else if (transactionType <= paymentWeight + stockLevelWeight + orderStatusWeight) {
                transactionTypeName = "Order-Status";
                BenchContext.CallbackContext context = new BenchContext.CallbackContext(this, TpccCommandType.ORDER_STATUS);
                Consumer tmpAccept = o -> callbackHandler.accept(o, context);
                doOrderStatus(tmpAccept);
            } else if (transactionType <= paymentWeight + stockLevelWeight + orderStatusWeight + deliveryWeight) {
                BenchContext.CallbackContext context = new BenchContext.CallbackContext(this, TpccCommandType.DELIVERY);
                Consumer tmpAccept = o -> callbackHandler.accept(o, context);
                transactionTypeName = "Delivery";
                doDelivery(tmpAccept);
            } else {
                transactionTypeName = "New-Order";
                BenchContext.CallbackContext context = new BenchContext.CallbackContext(this, TpccCommandType.NEW_ORDER);
                Consumer tmpAccept = o -> callbackHandler.accept(o, context);
                doNewOrder(tmpAccept);
                newOrderCounter++;
                newOrder = 1;
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
}
