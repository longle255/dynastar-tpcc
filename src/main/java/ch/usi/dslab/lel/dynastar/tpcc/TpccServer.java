package ch.usi.dslab.lel.dynastar.tpcc;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.bezerra.netwrapper.codecs.Codec;
import ch.usi.dslab.bezerra.netwrapper.codecs.CodecUncompressedKryo;
import ch.usi.dslab.lel.dynastar.tpcc.tables.*;
import ch.usi.dslab.lel.dynastar.tpcc.tpcc.TpccConfig;
import ch.usi.dslab.lel.dynastar.tpcc.tpcc.TpccUtil;
import ch.usi.dslab.lel.dynastarv2.Partition;
import ch.usi.dslab.lel.dynastarv2.PartitionStateMachine;
import ch.usi.dslab.lel.dynastarv2.command.Command;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
import ch.usi.dslab.lel.dynastarv2.probject.PRObject;
import ch.usi.dslab.lel.dynastarv2.probject.PRObjectGraph;
import ch.usi.dslab.lel.dynastarv2.probject.PRObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.BinaryJedis;

import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TpccServer extends PartitionStateMachine {
    public static final Logger log = LoggerFactory.getLogger(TpccServer.class);
    private AtomicInteger nextObjId = new AtomicInteger(0);
    private AtomicInteger exceptionCount = new AtomicInteger(0);

    public TpccServer(int replicaId, String systemConfig, String partitionsConfig, TpccProcedure appProcedure) {
        super(replicaId, systemConfig, partitionsConfig, appProcedure);
        this.setFeedbackInterval(50);
    }

    public static void main(String[] args) {
        log.debug("Entering main().");

        if (args.length != 9) {
            System.err.println("usage: replicaId systemConfigFile partitioningFile database gathererHost" +
                    "gathererPort monitorLogDir gathererDuration, gathererWarmup");
            System.exit(1);
        }
        int argIndex = 0;
        int replicaId = Integer.parseInt(args[argIndex++]);
        String systemConfigFile = args[argIndex++];
        String partitionsConfigFile = args[argIndex++];
        String database = args[argIndex++];
        String gathererHost = args[argIndex++];
        int gathererPort = Integer.parseInt(args[argIndex++]);
        String gathererDir = args[argIndex++];
        int gathererDuration = Integer.parseInt(args[argIndex++]);
        int gathererWarmup = Integer.parseInt(args[argIndex++]);
        TpccProcedure appProcedure = new TpccProcedure();
        TpccServer server = new TpccServer(replicaId, systemConfigFile, partitionsConfigFile, appProcedure);
        server.preLoadData(database, gathererHost);
        appProcedure.init("SERVER", server.objectGraph, server.secondaryIndex, logger, server.partitionId);
        server.setupMonitoring(gathererHost, gathererPort, gathererDir, gathererDuration, gathererWarmup);
        server.runStateMachine();

    }

    public void preLoadData(String file, String redisHost) {
        String hostName = null;
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        if (hostName.indexOf("node") == 0) {
            redisHost = "192.168.3.91";
        } else {
            redisHost = "127.0.0.1";
        }
        boolean cacheLoaded = false;
        long start = System.currentTimeMillis();
        String[] fileNameParts = file.split("/");
        String fileName = fileNameParts[fileNameParts.length - 1];
        Integer partitionCount = Partition.getPartitionsCount();
        log.info("Creating connection to redis host " + redisHost);
        BinaryJedis jedis = new BinaryJedis(redisHost, 6379, 600000);
        Codec codec = new CodecUncompressedKryo();
        byte[] keyObjectGraph = (fileName + "_p_" + partitionCount + "_SERVER_" + this.partitionId + "_objectGraph").getBytes();
        byte[] keySecondaryIndex = (fileName + "_p_" + partitionCount + "_SERVER_" + this.partitionId + "_secondaryIndex").getBytes();
        byte[] keyDataLoaded = (fileName + "_p_" + partitionCount + "_SERVER_" + this.partitionId + "_data_loaded").getBytes();
        try {
            byte[] cached = jedis.get(keyDataLoaded);
            if (cached != null && new String(cached).equals("OK")) {
                log.info("[SERVER" + this.partitionId + "] loading sample data from cache..." + System.currentTimeMillis());
                byte[] objectGraph = jedis.get(keyObjectGraph);
                byte[] secondaryIndex = jedis.get(keySecondaryIndex);
                log.info("[SERVER" + this.partitionId + "] reading data length: objectGraph=" + objectGraph.length + " - secondaryIndex=" + secondaryIndex.length);
                this.objectGraph = (PRObjectGraph) codec.createObjectFromBytes(objectGraph);
                this.secondaryIndex = (ConcurrentHashMap<String, Set<ObjId>>) codec.createObjectFromBytes(secondaryIndex);
                this.objectGraph.setLogger(this.logger);
                cacheLoaded = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        if (!cacheLoaded) {
            log.info("[SERVER" + this.partitionId + "] loading sample data from file..." + System.currentTimeMillis());
            TpccUtil.loadDataToCache(file, this.objectGraph, this.secondaryIndex, (objId, obj) -> {
                int dest = TpccUtil.mapIdToPartition(objId);
                if (nextObjId.get() <= objId.value) nextObjId.set(objId.value + 1);
//                if (obj.get("model").equals("Customer")) {
////                    System.out.println("[SERVER" + this.partitionId + "] indexing " + objId);
//                }
                if (this.partitionId == dest || obj.get("model").equals("Item")) {
                    TpccCommandPayload payload = new TpccCommandPayload(obj);
                    createObject(objId, payload);
//                    System.out    .println("[SERVER" + this.partitionId + "] indexing " + objId);
                } else {
                    PRObjectNode node = indexObject(objId, dest);
//                    log.info("[SERVER" + this.partitionId + "] indexing only " + objId);
                    node.setPartitionId(dest);
                }
            });
            byte[] objectGraph = codec.getBytes(this.objectGraph);
            byte[] secondaryIndex = codec.getBytes(this.secondaryIndex);
            log.info("[SERVER" + this.partitionId + "] writing data length: objectGraph=" + objectGraph.length + " - secondaryIndex=" + secondaryIndex.length);
            jedis.set(keyObjectGraph, objectGraph);
            jedis.set(keySecondaryIndex, codec.getBytes(this.secondaryIndex));
            jedis.set(keyDataLoaded, new String("OK").getBytes());
        }

        log.info("[SERVER" + this.partitionId + "] Data loaded, takes " + (System.currentTimeMillis() - start));
    }


    @Override
    protected PRObject createObject(PRObject prObject) {
//        log.debug("Creating user {}.", prObject.getId());
//        User newUser = new User(prObject.getId());
        return prObject;
    }

    @Override
    public Message executeCommand(Command command) {
        if (command.isInvalid()) {
            log.debug("cmd {} is invalid, causes transaction arboted");
            return new Message("TRANSACTION_ARBOTED");
        }
        command.rewind();
        // Command format : | byte op | ObjId o1 | int value |
        log.debug("cmd {} AppServer: Processing command {}-{}", command.getId(), command);
        TpccCommandType cmdType = (TpccCommandType) command.getItem(0);
        Map<String, Object> params = (Map<String, Object>) command.getItem(1);
        Set<ObjId> extractedObjId = appProcedure.extractObjectId(command);
        log.debug("cmd {} Extracted id: {}", command, extractedObjId);
        try {
            switch (cmdType) {
                case READ: {
                    String key = (String) params.get("key");
                    ObjId objId = secondaryIndex.get(key).iterator().next();
                    PRObjectNode node = this.objectGraph.getNode(objId);
                    log.debug("READ: {}", node);
                    log.debug("READ-Dependencies: {}", node.getDependencyIds());
                    return new Message(node);
                }
                case NEW_ORDER: {
                    int w_id = (int) params.get("w_id");
                    int d_id = (int) params.get("d_id");
                    int c_id = (int) params.get("c_id");
                    int o_ol_cnt = (int) params.get("ol_o_cnt");
                    int o_all_local = (int) params.get("o_all_local");
                    int[] itemIDs = (int[]) params.get("itemIds");
                    int[] supplierWarehouseIDs = (int[]) params.get("supplierWarehouseIDs");
                    int[] orderQuantities = (int[]) params.get("orderQuantities");
                    ObjId warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", w_id)).iterator().next();
                    ObjId districtObjId = secondaryIndex.get(Row.genSId("District", w_id, d_id)).iterator().next();
                    //TODO testing
//                    ObjId customerObjId = secondaryIndex.get(Row.genSId("Customer", w_id, d_id, c_id)).iterator().next();
                    ObjId customerObjId = new ObjId(Row.genSId("Customer", w_id, d_id, c_id));
                    log.debug("cmd {} NewOrder: w_id={} d_id={} c_id={} o_ol_cnt={} o_all_local={} itemIds={} supplierWarehouseIDs={} orderQuantities={}", command.getId(), w_id, d_id, c_id, o_ol_cnt, o_all_local, itemIDs, supplierWarehouseIDs, orderQuantities);

                    final List<Double> itemPrices = Arrays.asList(new Double[o_ol_cnt + 1]);
                    final List<String> itemNames = Arrays.asList(new String[o_ol_cnt + 1]);
                    final List<Double> orderLineAmounts = Arrays.asList(new Double[o_ol_cnt + 1]);
                    final List<Integer> stockQuantities = Arrays.asList(new Integer[o_ol_cnt + 1]);
                    final List<Character> brandGeneric = Arrays.asList(new Character[o_ol_cnt + 1]);


                    // The row in the WAREHOUSE table with matching W_ID is selected and W_TAX, the warehouse
                    // tax rate, is retrieved.
                    Warehouse warehouse = (Warehouse) getObject(warehouseObjId);
                    assert warehouse != null;

                    // The row in the DISTRICT table with matching D_W_ID and D_ ID is selected, D_TAX, the
                    // district tax rate, is retrieved, and D_NEXT_O_ID, the next available order number for
                    // the district, is retrieved and incremented by one.
                    District district = (District) getObject(districtObjId);
                    assert district != null;
                    PRObjectNode districtNode = this.objectGraph.getNode(districtObjId);
                    assert districtNode != null;
                    district.d_next_o_id += 1;

                    // The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is selected
                    // and C_DISCOUNT, the customer's discount rate, C_LAST, the customer's last name,
                    // and C_CREDIT, the customer's credit status, are retrieved.
                    Customer customer = (Customer) getObject(customerObjId);
                    assert customer != null;

                    // A new row is inserted into both the NEW-ORDER table and the ORDER table to reflect the
                    // creation of the new order. O_CARRIER_ID is set to a null value. If the order includes
                    // only home order-lines, then O_ALL_LOCAL is set to 1, otherwise O_ALL_LOCAL is set to 0.
                    NewOrder newOrder = new NewOrder(w_id, d_id, district.d_next_o_id);
                    newOrder.setId(new ObjId(nextObjId.getAndIncrement()));
                    PRObjectNode newOrderNode = indexObject(newOrder, this.partitionId);
                    newOrderNode.setPartitionId(this.partitionId);
                    districtNode.addDependencyIds(newOrderNode);
                    List<String> keys = Row.genStrObjId(newOrder.toHashMap());
//                    System.out.println("NewOrder Keys:" + newOrder.getId() + " - " + keys);
                    for (String key : keys) {
                        appProcedure.addToSecondaryIndex(this.secondaryIndex, key, newOrder.getId(), null);
                    }

                    Order order = new Order(w_id, d_id, c_id, district.d_next_o_id);
                    order.setId(new ObjId(nextObjId.getAndIncrement()));
                    order.o_entry_d = System.currentTimeMillis();
                    // The number of items, O_OL_CNT, is computed to match ol_cnt
                    order.o_ol_cnt = o_ol_cnt;
                    order.o_all_local = o_all_local;
                    PRObjectNode orderNode = indexObject(order, this.partitionId);
                    orderNode.setPartitionId(this.partitionId);
                    districtNode.addDependencyIds(orderNode);
                    keys = Row.genStrObjId(order.toHashMap());
                    for (String key : keys) {
//                        addToSecondaryIndex(key, order.getId());
                        appProcedure.addToSecondaryIndex(this.secondaryIndex, key, order.getId(), null);
                    }

                    // For each O_OL_CNT item on the order:
                    for (int index = 1; index <= o_ol_cnt; index++) {
                        int ol_number = index;
                        int ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
                        int ol_i_id = itemIDs[ol_number - 1];
                        int ol_quantity = orderQuantities[ol_number - 1];
                        // If I_ID has an unused value (see Clause 2.4.1.5), a "not-found" condition is signaled,
                        // resulting in a rollback of the database transaction (see Clause 2.4.2.3).
                        if (ol_i_id == -12345) {
                            // an expected condition generated 1% of the time in the test data...
                            // we throw an illegal access exception and the transaction gets rolled back later on
                            exceptionCount.getAndIncrement();
                        }

                        // The row in the ITEM table with matching I_ID (equals OL_I_ID) is selected
                        // and I_PRICE, the price of the item, I_NAME, the name of the item, and I_DATA are retrieved.
                        ObjId itemObjId = secondaryIndex.get(Row.genSId("Item", ol_i_id)).iterator().next();
                        Item item = (Item) getObject(itemObjId);
                        assert item != null;

                        itemPrices.set(ol_number - 1, item.i_price);
                        itemNames.set(ol_number - 1, item.i_name);

                        //TODO testing
//                        ObjId stockObjId = secondaryIndex.get(Row.genSId("Stock", ol_supply_w_id, ol_i_id)).iterator().next();
                        ObjId stockObjId = new ObjId(Row.genSId("Stock", ol_supply_w_id, ol_i_id));
                        Stock stock = (Stock) getObject(stockObjId);
                        assert stock != null;

                        int s_remote_cnt_increment;
//                    log.debug("cmd {} retriving stock w_id={} i_id={} stock {}", command.getId(),ol_supply_w_id, ol_i_id, stock);
                        stockQuantities.set(ol_number - 1, stock.s_quantity);
                        if (stock.s_quantity - ol_quantity >= 10) {
                            stock.s_quantity -= ol_quantity;
                        } else {
                            stock.s_quantity += -ol_quantity + 91;
                        }

                        if (ol_supply_w_id == w_id) {
                            s_remote_cnt_increment = 0;
                        } else {
                            s_remote_cnt_increment = 1;
                        }
                        stock.s_ytd += ol_quantity;
                        stock.s_remote_cnt += s_remote_cnt_increment;

                        double ol_amount = ol_quantity * item.i_price;
                        orderLineAmounts.set(ol_number - 1, ol_amount);

                        if (item.i_data.indexOf("GENERIC") != -1 && stock.s_data.indexOf("GENERIC") != -1) {
                            brandGeneric.set(ol_number - 1, 'B');
                        } else {
                            brandGeneric.set(ol_number - 1, 'G');
                        }
                        String ol_dist_info = null;
                        switch (district.d_id) {
                            case 1:
                                ol_dist_info = stock.s_dist_01;
                                break;
                            case 2:
                                ol_dist_info = stock.s_dist_02;
                                break;
                            case 3:
                                ol_dist_info = stock.s_dist_03;
                                break;
                            case 4:
                                ol_dist_info = stock.s_dist_04;
                                break;
                            case 5:
                                ol_dist_info = stock.s_dist_05;
                                break;
                            case 6:
                                ol_dist_info = stock.s_dist_06;
                                break;
                            case 7:
                                ol_dist_info = stock.s_dist_07;
                                break;
                            case 8:
                                ol_dist_info = stock.s_dist_08;
                                break;
                            case 9:
                                ol_dist_info = stock.s_dist_09;
                                break;
                            case 10:
                                ol_dist_info = stock.s_dist_10;

                                break;
                        }

                        OrderLine orderLine = new OrderLine(w_id, d_id, district.d_next_o_id, ol_number);
                        orderLine.setId(new ObjId(nextObjId.getAndIncrement()));
                        PRObjectNode orderLineNode = indexObject(orderLine, this.partitionId);
                        orderLineNode.setPartitionId(this.partitionId);
                        districtNode.addDependencyIds(orderLineNode);
                        keys = Row.genStrObjId(orderLine.toHashMap());
                        for (String key : keys) {
//                            addToSecondaryIndex(key, orderLine.getId());
                            appProcedure.addToSecondaryIndex(this.secondaryIndex, key, orderLine.getId(), null);
                        }
                        orderLine.ol_i_id = ol_i_id;
                        orderLine.ol_supply_w_id = ol_supply_w_id;
                        orderLine.ol_quantity = ol_quantity;
                        orderLine.ol_amount = ol_amount;
                        orderLine.ol_dist_info = ol_dist_info;

                        ((TpccProcedure) appProcedure).storeOrderLine(orderLine.getId());
                    }

//                return new Message(warehouse, district, customer, order, newOrder, itemPrices, itemNames, orderLineAmounts, stockQuantities, brandGeneric);
                    return new Message("OK");
                }
                case PAYMENT: {
                    int terminalWarehouseID = (int) params.get("w_id");
                    int districtID = (int) params.get("d_id");
                    ObjId warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
                    ObjId districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();

                    int customerWarehouseID = (int) params.get("c_w_id");
                    int customerDistrictID = (int) params.get("c_d_id");
//                    ObjId customerWarehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
//                    ObjId customerDistrictObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();

                    boolean customerByName = (boolean) params.get("c_by_name");
                    ObjId customerObjId;
                    int customerID = -1;
                    if (customerByName) customerObjId = (ObjId) params.get("c_objId");
                    else {
                        customerID = (int) params.get("c_id");
                        //todo testing
                        customerObjId = secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerID)).iterator().next();
//                        customerObjId = new ObjId(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerID));
                    }

                    float amount = (float) params.get("amount");
                    logger.debug("cmd {} PAYMENT: w_id={} d_id={} c_w_id={} c_d_id={} c_id={} customerByName={} customerObjId={}", command.getId(), terminalWarehouseID, districtID, customerWarehouseID, customerDistrictID, customerID, customerByName, customerObjId);

                    // The warehouse with matching w_id is selected. W_STREET_1, W_STREET_2, W_CITY, W_STATE, and W_ZIP are retrieved
                    // and W_YTD, the warehouse's year-to-date balance, is increased by H_ AMOUNT.
                    Warehouse warehouse = (Warehouse) getObject(warehouseObjId);
                    assert warehouse != null;
                    warehouse.w_ytd += amount;

                    // The district with matching D_W_ID and D_ID is selected. D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, and D_ZIP are retrieved
                    // and D_YTD, the district's year-to-date balance, is increased by H_AMOUNT.
                    District district = (District) getObject(districtObjId);
                    assert district != null;
                    PRObjectNode districtNode = this.objectGraph.getNode(districtObjId);
                    assert districtNode != null;
                    district.d_ytd += amount;

                    // Retrieve customer
                    Customer customer;
                    try {
                        customer = (Customer) getObject(customerObjId);
                    } catch (Exception e) {
                        log.error("cmd {} can't find customer", command.getId());
                        throw new RuntimeException("ERR");
                    }
                    assert customer != null;

                    customer.c_balance -= amount;
                    customer.c_payment_cnt += 1;
                    customer.c_ytd_payment += amount;

                    if (customer.c_credit.equals("BC")) {  // bad credit
                        String c_data = customer.c_data;
                        String c_new_data = customer.c_id + " " + customer.c_d_id + " " + customer.c_w_id + " " + districtID + " " + terminalWarehouseID + " " + amount + " |";
                        if (c_data.length() > c_new_data.length()) {
                            c_new_data += c_data.substring(0, c_data.length() - c_new_data.length());
                        } else {
                            c_new_data += c_data;
                        }
                        if (c_new_data.length() > 500) c_new_data = c_new_data.substring(0, 500);
                        customer.c_data = c_new_data;
                    }
                    String w_name = warehouse.w_name;
                    String d_name = district.d_name;
                    if (w_name.length() > 10) w_name = w_name.substring(0, 10);
                    if (d_name.length() > 10) d_name = d_name.substring(0, 10);
                    String h_data = w_name + "    " + d_name;
                    History history = new History(customer.c_id, customer.c_d_id, customer.c_w_id);
                    history.h_date = System.currentTimeMillis();
                    history.h_amount = amount;
                    history.h_data = h_data;
                    history.setId(new ObjId(nextObjId.getAndIncrement()));
                    PRObjectNode historyNode = indexObject(history, this.partitionId);
                    historyNode.setPartitionId(this.partitionId);
                    districtNode.addDependencyIds(historyNode);
                    return new Message("OK");
                }

                case ORDER_STATUS: {
                    int terminalWarehouseID = (int) params.get("w_id");
                    int districtID = (int) params.get("d_id");
                    boolean customerByName = (boolean) params.get("c_by_name");
                    ObjId customerObjId;
                    int customerID = -1;
                    if (customerByName) customerObjId = (ObjId) params.get("c_objId");
                    else {
                        customerID = (int) params.get("c_id");
//                        customerObjId = new ObjId(Row.genSId("Customer", terminalWarehouseID, districtID, customerID));
                        customerObjId = secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerID)).iterator().next();
                    }
                    ObjId orderObjId = (ObjId) params.get("orderObjId");
                    Set<ObjId> orderLineObjIds = (Set<ObjId>) params.get("orderLineObjIds");

                    if (orderObjId == null || orderLineObjIds == null) {
                        return new Message("Order is empty");
                    }
                    // Retrieve customer
                    Customer customer = (Customer) getObject(customerObjId);
                    assert customer != null;

                    Order order = (Order) getObject(orderObjId);
                    assert order != null;

                    Set<OrderLine> orderLines = new HashSet<>();
                    for (ObjId objId : orderLineObjIds) {
                        OrderLine orderLine = (OrderLine) getObject(objId);
                        assert orderLine != null;
                        orderLines.add(orderLine);
                    }

                    return new Message("OK");
                }

                case DELIVERY: {
                    int terminalWarehouseID = (int) params.get("w_id");
                    final ObjId orderIDs[] = new ObjId[TpccConfig.configDistPerWhse];
                    int o_carrier_id = (int) params.get("o_carrier_id");
                    ObjId warehouseObjId = new ObjId(Row.genSId("Warehouse", terminalWarehouseID));
//                    ObjId warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
                    Warehouse warehouse = (Warehouse) getObject(warehouseObjId);
                    assert warehouse != null;

                    for (int districtID = 1; districtID <= TpccConfig.configDistPerWhse; districtID++) {
                        ObjId districtObjId = new ObjId(Row.genSId("District", terminalWarehouseID, districtID));

//                        ObjId districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
                        District district = (District) getObject(districtObjId);
                        assert district != null;

                        // remove the oldest new order
                        ObjId no_objId_delete = (ObjId) params.get("newOrderObjId_" + districtID);
                        if (no_objId_delete == null) {
//                            System.out.println("w_id=" + terminalWarehouseID + ":d_id=" + districtID + " - partition " + getPartitionId() + " can't find " + ("newOrderObjId_" + districtID));
                            break;
                        }
//                        if (no_objId_delete==null)System.out.println("partition " + getPartitionId() + " can't find NewOrder for w=" + terminalWarehouseID + ":d=" + districtID);
                        orderIDs[districtID - 1] = no_objId_delete;

                        //TODO: find a work around for this: removed borrowed object
//                        markToRemove(this.objectGraph.getNode(no_objId_delete));

                        // find the corresponding order and update o_carrier_id
                        ObjId orderObjId = (ObjId) params.get("orderObjId_" + districtID);
                        if (orderObjId == null) {
                            System.out.println("w_id=" + terminalWarehouseID + ":d_id=" + districtID + " - partition " + getPartitionId() + " can't find " + Row.genSId("Order", terminalWarehouseID, districtID, -1, no_objId_delete));
                            break;
                        }
                        Order order = (Order) getObject(orderObjId);
                        assert order != null;
                        order.o_carrier_id = o_carrier_id;

                        //update corresponding orderline
                        Set<ObjId> orderLineObjIds = (Set<ObjId>) params.get("orderLineObjIds_" + districtID);
                        double total = 0;
                        for (ObjId orderLineObjId : orderLineObjIds) {
                            OrderLine orderLine = (OrderLine) getObject(orderLineObjId);
                            assert orderLine != null;
                            orderLine.ol_delivery_d = System.currentTimeMillis();
                            total += orderLine.ol_amount;
                        }

                        ObjId customerObjId = (ObjId) params.get("customerObjId_" + districtID);
                        Customer customer = (Customer) getObject(customerObjId);
                        assert customer != null;
                        customer.c_balance += total;
                    }

                    return new Message("OK");
                }

                case STOCK_LEVEL: {
                    int terminalWarehouseID = (int) params.get("w_id");
                    int districtID = (int) params.get("d_id");
                    int threshold = (int) params.get("threshold");
                    ObjId districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
//                    ObjId stockDistrictObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, TpccConfig.defautDistrictForStock)).iterator().next();
                    Set<ObjId> stockDistrictObjIds = TpccUtil.getStockDistrictId(terminalWarehouseID);
                    District district = (District) getObject(districtObjId);
                    for (ObjId id : stockDistrictObjIds) {
                        District stockDistrict = (District) getObject(id);
                        assert district != null;
                        PRObjectNode stockDistrictNode = objectGraph.getNode(id);
                    }
//                    System.out.println(stockDistrictObjId);
//                    System.out.println(stockDistrictNode.getDependencies());

                    String orderLineKey = Row.genSId("OrderLine", terminalWarehouseID, districtID);
                    Set<PRObject> stocks = new HashSet<>();
                    if (secondaryIndex.get(orderLineKey) != null) {
                        List<ObjId> orderLineObjIds = new ArrayList<>(secondaryIndex.get(orderLineKey));
//                    System.out.println("found orderLiness" + orderLineObjIds);
//                        Collections.sort(orderLineObjIds, new TpccProcedure.OrderLineOrderIdComparator());
//                    log.debug("found sorted orderLiness" + orderLineObjIds);
                        int i = 0;
                        while (i < 20 && orderLineObjIds.get(i) != null) {
//                        log.debug("partition {} getting obj {}", getPartitionId(), orderLineObjIds.get(i));
                            OrderLine orderLine = (OrderLine) getObject(orderLineObjIds.get(i));
                            assert orderLine != null;
//                        System.out.println(orderLine);
//                        System.out.println(orderLine.ol_i_id);
                            String s = Row.genSId("Stock", terminalWarehouseID, orderLine.ol_i_id);
//                            System.out.println(s);
//                        log.debug("found orderLine object {} \nStock ID {}\nIndex {}", orderLine, s);
                            if (secondaryIndex.containsKey(Row.genSId("Stock", terminalWarehouseID, orderLine.ol_i_id))) {
                                ObjId stockObjId = secondaryIndex.get(Row.genSId("Stock", terminalWarehouseID, orderLine.ol_i_id)).iterator().next();
                                Stock stock = (Stock) getObject(stockObjId);
                                assert stock != null;
                                try {
                                    if (stock.s_quantity < threshold) stocks.add(stock);
                                } catch (Exception e) {
                                    log.error("cmd {} failed. Can't find stock obj {}", command.getId(), stockObjId);
                                    return new Message("OK");
//                                    throw e;
                                }
                            } else {
                                log.debug("can't find stock for {}", Row.genSId("Stock", terminalWarehouseID, orderLine.ol_i_id));
                            }
                            i++;
                        }
                    }
                    log.debug("cmd {} STOCK_LEVEL: {}", command.getId(), stocks);
                    return new Message("OK");
                }
            }
        } catch (Exception e) {
//            log.error("cmd {}-{} hass error {}", command.getId(),command.toFullString(), e.getMessage());
            e.printStackTrace();
//            System.exit(1);
        }
        return new Message("??");
    }


    @Override
    public PRObject createObject(ObjId objId, Object value) {
        if (value instanceof TpccCommandPayload) {
            TpccCommandPayload payload = (TpccCommandPayload) value;
            String modelName = (String) payload.attributes.get("model");
            Row.MODEL modelObj = Row.getModelFromName(modelName);
//            log.debug("Creating object " + modelName + " with id=" + objId.value);
            String model = "ch.usi.dslab.lel.dynastar.tpcc.tables." + modelName;

            try {
                Class<?>[] params = Row.getConstructorParams();
                Object xyz = Class.forName(model).getConstructor(params).newInstance(objId);
                for (String attr : payload.attributes.keySet()) {
                    if (!attr.equals("ObjId") && !attr.equals("model")) {
                        Row.set(xyz, attr, payload.attributes.get(attr));
                    }
                }
                objId.setSId(((Row) xyz).getObjIdString());
                ((PRObject) xyz).setId(objId);
                PRObjectNode node = indexObject((PRObject) xyz, partitionId);
                node.setPartitionId(partitionId);
                if (!modelName.equals("District") && !modelName.equals("Warehouse") && !modelName.equals("Item")) {
                    node.setTransient(true);
                    int w_id = 0, d_id = 1;
                    if (modelName.equals("Customer")) {
                        w_id = ((Customer) xyz).c_w_id;
                        d_id = ((Customer) xyz).c_d_id;
                    } else if (modelName.equals("History")) {
                        w_id = ((History) xyz).h_c_w_id;
                        d_id = ((History) xyz).h_c_d_id;
                    } else if (modelName.equals("NewOrder")) {
                        w_id = ((NewOrder) xyz).no_w_id;
                        d_id = ((NewOrder) xyz).no_d_id;
                    } else if (modelName.equals("Order")) {
                        w_id = ((Order) xyz).o_w_id;
                        d_id = ((Order) xyz).o_d_id;
                    } else if (modelName.equals("OrderLine")) {
                        w_id = ((OrderLine) xyz).ol_w_id;
                        d_id = ((OrderLine) xyz).ol_d_id;
                    } else if (modelName.equals("Stock")) {
                        w_id = ((Stock) xyz).s_w_id;
                        d_id = TpccUtil.mapStockToDistrict(objId.getSId());
                    }
                    try {
                        ObjId districtObjId = secondaryIndex.get(Row.genSId("District", w_id, d_id)).iterator().next();
                        PRObjectNode districtNode = this.objectGraph.getNode(districtObjId);
                        assert districtNode != null;
                        log.debug("Creating object {} with id={} sid={} belongs to {}", modelName, objId.value, objId.getSId(), districtObjId);
                        districtNode.addDependencyIds(node);
                    } catch (Exception e) {
                        System.out.println("ERROR getting " + Row.genSId("District", w_id, d_id));
                    }
                } else if (modelName.equals("Item")) {
                    node.setTransient(true);
                    node.setReplicated(true);
                } else {
                    log.debug("Creating object {} with id={} sId={}", modelName, objId.value, objId.getSId());
                }
                return (PRObject) xyz;
            } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | ClassNotFoundException | InvocationTargetException | NullPointerException e) {
                e.printStackTrace();
            }
            return null;
        } else {
            objId.setSId(((Row) value).getObjIdString());
            ((PRObject) value).setId(objId);
            PRObjectNode node = indexObject((PRObject) value, partitionId);
            node.setPartitionId(partitionId);
            return (PRObject) value;
        }
    }


}
