package ch.usi.dslab.lel.dynastar.tpcc;

import ch.usi.dslab.lel.dynastar.tpcc.tables.*;
import ch.usi.dslab.lel.dynastar.tpcc.tpcc.TpccConfig;
import ch.usi.dslab.lel.dynastar.tpcc.tpcc.TpccUtil;
import ch.usi.dslab.lel.dynastarv2.AppProcedure;
import ch.usi.dslab.lel.dynastarv2.StateMachine;
import ch.usi.dslab.lel.dynastarv2.command.Command;
import ch.usi.dslab.lel.dynastarv2.messages.CommandType;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
import ch.usi.dslab.lel.dynastarv2.probject.PRObject;
import ch.usi.dslab.lel.dynastarv2.probject.PRObjectGraph;
import ch.usi.dslab.lel.dynastarv2.probject.PRObjectNode;
import com.google.common.base.Splitter;
import org.slf4j.Logger;

import java.util.*;

public class TpccProcedure implements AppProcedure {
    //    public TpccProcedure(PRObjectGraph graph, Map<String, Set<ObjId>> secondaryIndex){
//        setCache(graph, secondaryIndex);
//    }

    private static Splitter splitterCollon = Splitter.on(':');
    private static Splitter splitterEqual = Splitter.on('=');
    //    Queue<ObjId> mostRecentOrderdItem = EvictingQueue.create(15 * TpccConfig.configDistPerWhse);
//    Queue<ObjId> mostRecentOrderLine = EvictingQueue.create(15 * TpccConfig.configDistPerWhse);
    private PRObjectGraph graph;
    private Map<String, Set<ObjId>> secondaryIndex;
    private String role;
    private Logger logger;
    private int partitionId;

    static String safeKey(String model, String... parts) {
        StringBuilder ret = new StringBuilder(model);
        for (int i = 0; i < parts.length; i++) {
            ret.append(":");
            ret.append(parts[i]);
            ret.append("=");
            ret.append(parts[++i]);
        }
        return ret.toString();
    }

    @Override
    public boolean isTransient(PRObjectNode node) {
        return isTransient(node.getId());
    }

    public void init(String role, PRObjectGraph graph, Map<String, Set<ObjId>> secondaryIndex, Logger logger, int partitionId) {
        this.role = role;
        this.graph = graph;
        this.secondaryIndex = secondaryIndex;
        this.logger = logger;
        this.partitionId = partitionId;
    }

    public void storeOrderLine(ObjId id) {
//        mostRecentOrderLine.add(id);
    }

    public boolean shouldQuery(PRObjectGraph cache, Command command, boolean forceQuery) {
        if (command.getReservedObjects() != null) return true;
        if (command.getItem(0) instanceof CommandType && command.getItem(0) == CommandType.CREATE) return true;
        if (command.getItem(0) instanceof TpccCommandType) {
            TpccCommandType cmdType = (TpccCommandType) command.getItem(0);
            Map<String, Object> params = (Map<String, Object>) command.getItem(1);
            switch (cmdType) {
                case NEW_ORDER:
                case STOCK_LEVEL:
                case ORDER_STATUS:
                case PAYMENT: {
                    int terminalWarehouseID = (int) params.get("w_id");
                    int districtId = (int) params.get("d_id");
                    ObjId districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtId)).iterator().next();
                    if (graph.getNode(districtObjId) != null) return false;
                    break;
                }
                case DELIVERY: {
                    int terminalWarehouseID = (int) params.get("w_id");
                    ObjId warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
                    if (graph.getNode(warehouseObjId) != null) return false;
                    break;
                }
                default: {
                    return true;
                }
            }
        }
        return true;
    }

    @Override
    public void calculateMovingPlan(Command commandWrapper) {
        int destPartId = (int) commandWrapper.getNext();
        Map<Integer, Set<ObjId>> srcPartMap = (HashMap) commandWrapper.getNext();
        Command command = (Command) commandWrapper.getNext();
        Set<ObjId> existing = new HashSet<>();
        Set<ObjId> missing = new HashSet<>();

        try {
            TpccCommandType cmdType = (TpccCommandType) command.getNext();
            Map<String, Object> params = (Map<String, Object>) command.getItem(1);

            switch (cmdType) {
                case NEW_ORDER: {
                    int terminalWarehouseID = (int) params.get("w_id");
                    int districtID = (int) params.get("d_id");
                    int customerID = (int) params.get("c_id");
                    int numItems = (int) params.get("ol_o_cnt");
                    int allLocal = (int) params.get("o_all_local");
                    int[] itemIDs = (int[]) params.get("itemIds");
                    int[] supplierWarehouseIDs = (int[]) params.get("supplierWarehouseIDs");
                    int[] orderQuantities = (int[]) params.get("orderQuantities");

                    ObjId warehouseObjId = null, districtObjId = null, customerObjId = null;
                    if (secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)) != null && secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).size() > 0)
                        warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();

                    if (warehouseObjId != null && graph.getNode(warehouseObjId) != null && graph.getNode(warehouseObjId).getOwnerPartitionId() == this.partitionId)
                        existing.add(warehouseObjId);
                    else
                        missing.add(new ObjId(Row.genSId("Warehouse", terminalWarehouseID)));

                    if (secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)) != null && secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).size() > 0)
                        districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
                    if (districtObjId != null && graph.getNode(districtObjId) != null && graph.getNode(districtObjId).getOwnerPartitionId() == this.partitionId)
                        existing.add(districtObjId);
                    else
                        missing.add(new ObjId(Row.genSId("District", terminalWarehouseID, districtID)));

                    if (secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerID)) != null && secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerID)).size() > 0)
                        customerObjId = secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerID)).iterator().next();
                    if (customerObjId != null && graph.getNode(customerObjId) != null && graph.getNode(customerObjId).getOwnerPartitionId() == this.partitionId)
                        existing.add(customerObjId);
                    else
                        missing.add(new ObjId(Row.genSId("Customer", terminalWarehouseID, districtID, customerID)));


                    Set<ObjId> itemObjIds = new HashSet<>();
                    for (int i = 0; i < numItems; i++) {
                        if (!secondaryIndex.containsKey(Row.genSId("Item", itemIDs[i]))) {
                            command.setInvalid(true);
                            commandWrapper.setInvalid(true);
                            return;
                        }
                        if (secondaryIndex.get(Row.genSId("Item", itemIDs[i])) != null && secondaryIndex.get(Row.genSId("Item", itemIDs[i])).size() > 0)
                            itemObjIds.add(secondaryIndex.get(Row.genSId("Item", itemIDs[i])).iterator().next());
                    }

                    for (int i = 0; i < numItems; i++) {
                        ObjId supplierWarehouseObjId = null;
                        if (secondaryIndex.get(Row.genSId("Warehouse", supplierWarehouseIDs[i])) != null && secondaryIndex.get(Row.genSId("Warehouse", supplierWarehouseIDs[i])).size() > 0)
                            supplierWarehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", supplierWarehouseIDs[i])).iterator().next();

                        if (supplierWarehouseObjId != null && graph.getNode(supplierWarehouseObjId) != null && graph.getNode(supplierWarehouseObjId).getOwnerPartitionId() == this.partitionId)
                            existing.add(supplierWarehouseObjId);
                        else
                            missing.add(new ObjId(Row.genSId("Warehouse", supplierWarehouseIDs[i])));
                    }


                    for (int i = 0; i < numItems; i++) {
                        ObjId stockObjId = null;
                        if (secondaryIndex.get(Row.genSId("Stock", supplierWarehouseIDs[i], itemIDs[i])) != null && secondaryIndex.get(Row.genSId("Stock", supplierWarehouseIDs[i], itemIDs[i])).size() > 0)
                            stockObjId = secondaryIndex.get(Row.genSId("Stock", supplierWarehouseIDs[i], itemIDs[i])).iterator().next();

                        if (stockObjId != null && graph.getNode(stockObjId) != null && graph.getNode(stockObjId).getOwnerPartitionId() == this.partitionId)
                            existing.add(stockObjId);
                        else
                            missing.add(new ObjId(Row.genSId("Stock", supplierWarehouseIDs[i], itemIDs[i])));
                    }
                    break;
                }
                case PAYMENT: {
                    int terminalWarehouseID = (int) params.get("w_id");
                    int districtID = (int) params.get("d_id");

                    ObjId warehouseObjId = null, districtObjId = null;
                    if (secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)) != null && secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).size() > 0)
                        warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();

                    if (warehouseObjId != null && graph.getNode(warehouseObjId) != null && graph.getNode(warehouseObjId).getOwnerPartitionId() == this.partitionId)
                        existing.add(warehouseObjId);
                    else
                        missing.add(new ObjId(Row.genSId("Warehouse", terminalWarehouseID)));

                    if (secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)) != null && secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).size() > 0)
                        districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
                    if (districtObjId != null && graph.getNode(districtObjId) != null && graph.getNode(districtObjId).getOwnerPartitionId() == this.partitionId)
                        existing.add(districtObjId);
                    else
                        missing.add(new ObjId(Row.genSId("District", terminalWarehouseID, districtID)));


                    int customerWarehouseID = (int) params.get("c_w_id");
                    int customerDistrictID = (int) params.get("c_d_id");
                    ObjId customerWarehouseObjId = null, customerDistrictObjId = null;

                    if (secondaryIndex.get(Row.genSId("Warehouse", customerWarehouseID)) != null && secondaryIndex.get(Row.genSId("Warehouse", customerWarehouseID)).size() > 0)
                        customerWarehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", customerWarehouseID)).iterator().next();

                    if (customerWarehouseObjId != null && graph.getNode(customerWarehouseObjId) != null && graph.getNode(customerWarehouseObjId).getOwnerPartitionId() == this.partitionId)
                        existing.add(customerWarehouseObjId);
                    else
                        missing.add(new ObjId(Row.genSId("Warehouse", customerWarehouseID)));

                    if (secondaryIndex.get(Row.genSId("District", customerWarehouseID, customerDistrictID)) != null && secondaryIndex.get(Row.genSId("District", customerWarehouseID, customerDistrictID)).size() > 0)
                        customerDistrictObjId = secondaryIndex.get(Row.genSId("District", customerWarehouseID, customerDistrictID)).iterator().next();
                    if (customerDistrictObjId != null && graph.getNode(customerDistrictObjId) != null && graph.getNode(customerDistrictObjId).getOwnerPartitionId() == this.partitionId)
                        existing.add(customerDistrictObjId);
                    else
                        missing.add(new ObjId(Row.genSId("District", customerWarehouseID, customerDistrictID)));


                    boolean customerByName = (boolean) params.get("c_by_name");
                    if (!customerByName) {
                        int customerID = (int) params.get("c_id");
                        ObjId customerObjId = null;
                        if (secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerID)) != null && secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerID)).size() > 0)
                            customerObjId = secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerID)).iterator().next();
                        if (customerObjId != null && graph.getNode(customerObjId) != null && graph.getNode(customerObjId).getOwnerPartitionId() == this.partitionId)
                            existing.add(customerObjId);
                        else
                            missing.add(new ObjId(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerID)));
                    } else {
                        long seed = (long) params.get("randomSeed");
                        Random r = new Random(seed);
                        String customerLastName = TpccUtil.getLastName(r);

                        ObjId customerObjId = secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerLastName)) != null ?
                                secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerLastName)).iterator().next() : null;
                        while (customerObjId == null) {
                            customerLastName = TpccUtil.getLastName(r);
                            if (secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerLastName)) == null)
                                continue;
                            customerObjId = secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerLastName)).iterator().next();
                        }

                        if (customerObjId != null && graph.getNode(customerObjId) != null && graph.getNode(customerObjId).getOwnerPartitionId() == this.partitionId)
                            existing.add(customerObjId);
                        else
                            missing.add(customerObjId);
                    }
                    break;
                }
                case ORDER_STATUS: {
                    int terminalWarehouseID = (int) params.get("w_id");
                    int districtID = (int) params.get("d_id");


                    ObjId warehouseObjId = null, districtObjId = null;
                    if (secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)) != null && secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).size() > 0)
                        warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();

                    if (warehouseObjId != null && graph.getNode(warehouseObjId) != null && graph.getNode(warehouseObjId).getOwnerPartitionId() == this.partitionId)
                        existing.add(warehouseObjId);
                    else
                        missing.add(new ObjId(Row.genSId("Warehouse", terminalWarehouseID)));

                    if (secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)) != null && secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).size() > 0)
                        districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
                    if (districtObjId != null && graph.getNode(districtObjId) != null && graph.getNode(districtObjId).getOwnerPartitionId() == this.partitionId)
                        existing.add(districtObjId);
                    else
                        missing.add(new ObjId(Row.genSId("District", terminalWarehouseID, districtID)));


                    boolean customerByName = (boolean) params.get("c_by_name");
                    ObjId customerObjId = null;
                    if (!customerByName) {
                        int customerID = (int) params.get("c_id");

                        if (secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerID)) != null && secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerID)).size() > 0)
                            customerObjId = secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerID)).iterator().next();
                        if (customerObjId != null && graph.getNode(customerObjId) != null && graph.getNode(customerObjId).getOwnerPartitionId() == this.partitionId)
                            existing.add(customerObjId);
                        else
                            missing.add(new ObjId(Row.genSId("Customer", terminalWarehouseID, districtID, customerID)));
                    } else {
                        long seed = (long) params.get("randomSeed");
                        Random r = new Random(seed);
                        String customerLastName = TpccUtil.getLastName(r);

                        customerObjId = secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerLastName)) != null ?
                                secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerLastName)).iterator().next() : null;
                        while (customerObjId == null) {
                            customerLastName = TpccUtil.getLastName(r);
                            if (secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerLastName)) == null)
                                continue;
                            customerObjId = secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerLastName)).iterator().next();
                        }

                        if (customerObjId != null && graph.getNode(customerObjId) != null && graph.getNode(customerObjId).getOwnerPartitionId() == this.partitionId)
                            existing.add(customerObjId);
                        else
                            missing.add(customerObjId);
                    }

                    String ordersKey = Row.genSId("Order", terminalWarehouseID, districtID, customerObjId.getSId().split(":")[3].split("=")[1]);

                    if (secondaryIndex.get(ordersKey) != null) {
                        ObjId orderObjId = new ObjId(ordersKey);

                        if (orderObjId != null && graph.getNode(orderObjId) != null && graph.getNode(orderObjId).getOwnerPartitionId() == this.partitionId)
                            existing.add(orderObjId);
                        else
                            missing.add(orderObjId);

                        String orderLineKey = Row.genSId("OrderLine", terminalWarehouseID, districtID, orderObjId.getSId().split(":")[4].split("=")[1]);
                        if (secondaryIndex.get(orderLineKey) != null) {
                            Set<ObjId> orderLines = secondaryIndex.get(orderLineKey);
                            params.put("orderLineObjIds", orderLines);
                            for (ObjId orderLineObjId : orderLines) {
                                if (orderLineObjId != null && graph.getNode(orderLineObjId) != null && graph.getNode(orderLineObjId).getOwnerPartitionId() == this.partitionId)
                                    existing.add(orderLineObjId);
                                else
                                    missing.add(orderLineObjId);

                            }
                        }
                    }
                    break;
                }
                case DELIVERY: {
                    int terminalWarehouseID = (int) params.get("w_id");
                    ObjId warehouseObjId = null;
                    if (secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)) != null && secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).size() > 0)
                        warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();

                    if (warehouseObjId != null && graph.getNode(warehouseObjId) != null && graph.getNode(warehouseObjId).getOwnerPartitionId() == this.partitionId)
                        existing.add(warehouseObjId);
                    else
                        missing.add(new ObjId(Row.genSId("Warehouse", terminalWarehouseID)));


                    for (int districtID = 1; districtID <= TpccConfig.configDistPerWhse; districtID++) {
                        ObjId districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
                        if (secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)) != null && secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).size() > 0)
                            districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
                        if (districtObjId != null && graph.getNode(districtObjId) != null && graph.getNode(districtObjId).getOwnerPartitionId() == this.partitionId)
                            existing.add(districtObjId);
                        else
                            missing.add(districtObjId);


                        String newOrdersKey = Row.genSId("NewOrder", terminalWarehouseID, districtID);
                        boolean found = false;
                        if (secondaryIndex.get(newOrdersKey) != null) {
                            found = true;
                            ObjId newOrderObjId;
                            newOrderObjId = secondaryIndex.get(newOrdersKey).iterator().next();

                            if (newOrderObjId != null && graph.getNode(newOrderObjId) != null && graph.getNode(newOrderObjId).getOwnerPartitionId() == this.partitionId)
                                existing.add(newOrderObjId);
                            else
                                missing.add(newOrderObjId);

                            String orderIdKey = Row.genSId("Order", terminalWarehouseID, districtID, -1, newOrderObjId.getSId().split(":")[3].split("=")[1]);

                            if (secondaryIndex.get(orderIdKey) == null) {
                                System.out.println("ERROR: Can't find order " + orderIdKey);
                                command.setInvalid(true);
                                return;
                            }

                            ObjId orderObjId = secondaryIndex.get(orderIdKey).iterator().next();
                            if (orderObjId != null && graph.getNode(orderObjId) != null && graph.getNode(orderObjId).getOwnerPartitionId() == this.partitionId)
                                existing.add(orderObjId);
                            else
                                missing.add(orderObjId);

                            String customerKey = Row.genSId("Customer", terminalWarehouseID, districtID, Integer.parseInt(orderObjId.getSId().split(":")[3].split("=")[1]));
                            ObjId customerObjId = secondaryIndex.get(customerKey).iterator().next();

                            if (secondaryIndex.get(customerObjId) != null && secondaryIndex.get(customerObjId).size() > 0)
                                customerObjId = secondaryIndex.get(customerObjId).iterator().next();
                            if (customerObjId != null && graph.getNode(customerObjId) != null && graph.getNode(customerObjId).getOwnerPartitionId() == this.partitionId)
                                existing.add(customerObjId);
                            else
                                missing.add(customerObjId);


                            String orderLineKey = Row.genSId("OrderLine", terminalWarehouseID, districtID, orderObjId.getSId().split(":")[4].split("=")[1]);
                            if (secondaryIndex.get(orderLineKey) != null) {
                                Set<ObjId> orderLines = secondaryIndex.get(orderLineKey);
                                for (ObjId orderLineObjId : orderLines) {
                                    if (orderLineObjId != null && graph.getNode(orderLineObjId) != null && graph.getNode(orderLineObjId).getOwnerPartitionId() == this.partitionId)
                                        existing.add(orderLineObjId);
                                    else
                                        missing.add(orderLineObjId);

                                }
                            }
                        }
                    }

                    break;
                }
                case STOCK_LEVEL: {
                    int terminalWarehouseID = (int) params.get("w_id");
                    int districtID = (int) params.get("d_id");
                    ObjId warehouseObjId = null, districtObjId = null;
                    if (secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)) != null && secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).size() > 0)
                        warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();

                    if (warehouseObjId != null && graph.getNode(warehouseObjId) != null && graph.getNode(warehouseObjId).getOwnerPartitionId() == this.partitionId)
                        existing.add(warehouseObjId);
                    else
                        missing.add(new ObjId(Row.genSId("Warehouse", terminalWarehouseID)));

                    if (secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)) != null && secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).size() > 0)
                        districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
                    if (districtObjId != null && graph.getNode(districtObjId) != null && graph.getNode(districtObjId).getOwnerPartitionId() == this.partitionId)
                        existing.add(districtObjId);
                    else
                        missing.add(new ObjId(Row.genSId("District", terminalWarehouseID, districtID)));

                    Set<ObjId> stockDistrictObjIds = TpccUtil.getStockDistrictId(terminalWarehouseID);
                    stockDistrictObjIds.forEach(objId -> objId.includeDependencies = true);
                    for (ObjId districtId : stockDistrictObjIds) {
                        if (districtId != null && graph.getNode(districtId) != null && graph.getNode(districtId).getOwnerPartitionId() == this.partitionId)
                            existing.add(districtId);
                        else
                            missing.add(districtId);
                    }

                    break;
                }
                default:
                    break;
            }
        } catch (Exception e) {
            System.out.println("P_" + this.partitionId + " ERROR: " + e.getMessage() + " command " + command.toFullString());
            e.printStackTrace();
        }

        if (destPartId == this.partitionId) {
            logger.debug("cmd {} is going to execute on THIS partition. Will calculate what to receive {}", command.getId(), missing);
            srcPartMap.put(-1, missing);
        } else {
            logger.debug("cmd {} is going to execute on ANOTHER partition. Will calculate what to send {}", command.getId(), existing);
            srcPartMap.put(this.partitionId, existing);
        }


        return;
    }


    public Set<ObjId> extractObjectIdForMoving(Command command) {

        Set<ObjId> ret = new HashSet<>();

        try {
            TpccCommandType cmdType = (TpccCommandType) command.getNext();
            Map<String, Object> params = (Map<String, Object>) command.getItem(1);

            switch (cmdType) {
                case READ: {
                    ObjId key = secondaryIndex.get(params.get("key")).iterator().next();
                    ret.add(key);
                    break;
                }
                case NEW_ORDER: {
                    int terminalWarehouseID = (int) params.get("w_id");
                    int districtID = (int) params.get("d_id");
                    int customerID = (int) params.get("c_id");
                    int numItems = (int) params.get("ol_o_cnt");
                    int allLocal = (int) params.get("o_all_local");
                    int[] itemIDs = (int[]) params.get("itemIds");
                    int[] supplierWarehouseIDs = (int[]) params.get("supplierWarehouseIDs");
                    int[] orderQuantities = (int[]) params.get("orderQuantities");
//                        System.out.println("All local - "+allLocal+" - w_id="+terminalWarehouseID+" - d_id="+districtID+" - supplierWarehouseIDs="+supplierWarehouseIDs[0]);
                    ObjId warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
                    ObjId districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();

                    ObjId customerObjId = new ObjId(Row.genSId("Customer", terminalWarehouseID, districtID, customerID));
                    //TODO testing
//                        ObjId customerObjId = secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerID)).iterator().next();
                    Set<ObjId> itemObjIds = new HashSet<>();
                    for (int i = 0; i < numItems; i++) {
                        if (!secondaryIndex.containsKey(Row.genSId("Item", itemIDs[i]))) {
                            command.setInvalid(true);
                            return ret;
                        }
                        itemObjIds.add(secondaryIndex.get(Row.genSId("Item", itemIDs[i])).iterator().next());
                    }
                    Set<ObjId> supplierWarehouseObjIds = new HashSet<>();
                    for (int i = 0; i < numItems; i++) {
                        //TODO testing
                        supplierWarehouseObjIds.add(secondaryIndex.get(Row.genSId("Warehouse", supplierWarehouseIDs[i])).iterator().next());
//                            supplierWarehouseObjIds.add(new ObjId(Row.genSId("Warehouse", supplierWarehouseIDs[i])));
                    }

                    Set<ObjId> stockObjIds = new HashSet<>();
                    for (int i = 0; i < numItems; i++) {
                        //TODO testing
//                            stockObjIds.add(secondaryIndex.get(Row.genSId("Stock", supplierWarehouseIDs[i], itemIDs[i])).iterator().next());
                        stockObjIds.add(new ObjId(Row.genSId("Stock", supplierWarehouseIDs[i], itemIDs[i])));
                    }
                    ret.add(warehouseObjId);
                    ret.add(districtObjId);
                    ret.add(customerObjId);
                    ret.addAll(itemObjIds);
                    ret.addAll(supplierWarehouseObjIds);
                    ret.addAll(stockObjIds);
//                    mostRecentOrderdItem.addAll(stockObjIds);
                    break;
                }
                case PAYMENT: {
                    int terminalWarehouseID = (int) params.get("w_id");
                    int districtID = (int) params.get("d_id");
                    ObjId warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
                    ObjId districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
                    ret.add(warehouseObjId);
                    ret.add(districtObjId);

                    int customerWarehouseID = (int) params.get("c_w_id");
                    int customerDistrictID = (int) params.get("c_d_id");

                    ObjId customerWarehouseObjId = new ObjId(Row.genSId("Warehouse", terminalWarehouseID));
                    ObjId customerDistrictObjId = new ObjId(Row.genSId("District", terminalWarehouseID, districtID));
//                        ObjId customerWarehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
//                        ObjId customerDistrictObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
                    ret.add(customerWarehouseObjId);
                    ret.add(customerDistrictObjId);

                    boolean customerByName = (boolean) params.get("c_by_name");
                    if (!customerByName) {
                        int customerID = (int) params.get("c_id");

//                            ObjId customerObjId = new ObjId(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerID));
//                            logger.info("P_{} looking for customer {}", this.partitionId, Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerID));
                        ObjId customerObjId = secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerID)).iterator().next();
                        ret.add(customerObjId);
                    } else {
                        long seed = (long) params.get("randomSeed");
                        Random r = new Random(seed);
                        String customerLastName = TpccUtil.getLastName(r);
//                            ObjId customerObjId = new ObjId(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerID));
                        ObjId customerObjId = secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerLastName)) != null ?
                                secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerLastName)).iterator().next() : null;
                        while (customerObjId == null) {
                            customerLastName = TpccUtil.getLastName(r);
//                            System.out.println("looking for " + customerLastName);
                            if (secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerLastName)) == null)
                                continue;
                            customerObjId = secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerLastName)).iterator().next();
//                            System.out.println("found " + customerLastName + " - " + customerObjId);
                        }
                        params.put("c_objId", customerObjId);
                        ret.add(customerObjId);
                    }
                    break;
                }
                case ORDER_STATUS: {
                    int terminalWarehouseID = (int) params.get("w_id");
                    int districtID = (int) params.get("d_id");
                    ObjId warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
                    ObjId districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
                    ret.add(warehouseObjId);
                    ret.add(districtObjId);
                    boolean customerByName = (boolean) params.get("c_by_name");
                    ObjId customerObjId;
                    if (!customerByName) {
                        int customerID = (int) params.get("c_id");
                        customerObjId = secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerID)).iterator().next();
                        ret.add(customerObjId);
                    } else {
                        long seed = (long) params.get("randomSeed");
                        Random r = new Random(seed);
                        String customerLastName = TpccUtil.getLastName(r);
                        customerObjId = secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerLastName)) != null ?
                                secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerLastName)).iterator().next() : null;
                        while (customerObjId == null) {
                            customerLastName = TpccUtil.getLastName(r);
//                            System.out.println("looking for "+customerLastName);
                            if (secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerLastName)) == null)
                                continue;
                            customerObjId = secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerLastName)).iterator().next();
//                            System.out.println("found "+customerLastName+" - "+customerObjId);
                        }
                        params.put("c_objId", customerObjId);
                        ret.add(customerObjId);
                    }

//                    System.out.println("customer id " + customerObjId.getSId());
//                    System.out.println("extracted id: " + customerObjId.getSId().split(":")[3].split("=")[1]);
                    String ordersKey = Row.genSId("Order", terminalWarehouseID, districtID, customerObjId.getSId().split(":")[3].split("=")[1]);
//                    System.out.println("finding keys " + ordersKey);

                    if (secondaryIndex.get(ordersKey) != null) {
//                            List<ObjId> orders = new ArrayList<>();
//                            synchronized (secondaryIndex.get(ordersKey)) {
//                                orders.addAll(secondaryIndex.get(ordersKey));
//                            }
////                            orders.addAll(secondaryIndex.get(ordersKey));
//                            Collections.sort(orders, new OrderIdComparator());
//
//                        System.out.println("found orders" + orders);
                        ObjId orderObjId = new ObjId(ordersKey);
//                            ObjId orderObjId = secondaryIndex.get(ordersKey).iterator().next();
                        params.put("orderObjId", orderObjId);
                        ret.add(orderObjId);
                        String orderLineKey = Row.genSId("OrderLine", terminalWarehouseID, districtID, orderObjId.getSId().split(":")[4].split("=")[1]);
//                        System.out.println("finding ol keys " + orderLineKey);
                        if (secondaryIndex.get(orderLineKey) != null) {
                            Set<ObjId> orderLines = secondaryIndex.get(orderLineKey);
//                            System.out.println("found orderLiness" + orderLines);
                            params.put("orderLineObjIds", orderLines);
                            ret.addAll(orderLines);
                        }
                    }
                    break;
                }
                case DELIVERY: {
                    int terminalWarehouseID = (int) params.get("w_id");
                    ObjId warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
                    ret.add(warehouseObjId);
                    for (int districtID = 1; districtID <= TpccConfig.configDistPerWhse; districtID++) {
                        ObjId districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
                        ret.add(districtObjId);

                        String newOrdersKey = Row.genSId("NewOrder", terminalWarehouseID, districtID);
//                            System.out.println("finding newOrdersKey keys " + newOrdersKey);
                        boolean found = false;
                        if (secondaryIndex.get(newOrdersKey) != null) {
                            found = true;
//                                List<ObjId> newOrders = new ArrayList<>();
                            ObjId newOrderObjId;
//                                synchronized (secondaryIndex.get(newOrdersKey)) {
//                                    newOrders.addAll(secondaryIndex.get(newOrdersKey));
                            newOrderObjId = secondaryIndex.get(newOrdersKey).iterator().next();
//                                }
//                                System.out.println("found newOrders sorted" + newOrderObjId);
//                                Collections.sort(newOrders, new NewOrderIdComparator());
//                            System.out.println("found newOrders" + newOrders);
//                                newOrderObjId = newOrders.iterator().next();
                            params.put("newOrderObjId_" + districtID, newOrderObjId);
                            ret.add(newOrderObjId);


                            String orderIdKey = Row.genSId("Order", terminalWarehouseID, districtID, -1, newOrderObjId.getSId().split(":")[3].split("=")[1]);
//                            System.out.println("orderId key" + orderIdKey);

                            if (secondaryIndex.get(orderIdKey) == null) {
                                System.out.println("ERROR: Can't find order " + orderIdKey);
                                command.setInvalid(true);
                                return ret;
                            }
                            ObjId orderObjId = secondaryIndex.get(orderIdKey).iterator().next();
//                                System.out.println("found order id" + orderObjId);
                            params.put("orderObjId_" + districtID, orderObjId);
                            ret.add(orderObjId);

                            String customerKey = Row.genSId("Customer", terminalWarehouseID, districtID, Integer.parseInt(orderObjId.getSId().split(":")[3].split("=")[1]));
//                                System.out.println("customerKey " + customerKey);
//                                if (!secondaryIndex.containsKey(customerKey))
//                                    System.out.println("w_id=" + warehouseObjId + "d_id=" + districtID + "-" + orderObjId + "-can't find customerKey " + customerKey);
                            ObjId customerObjId = secondaryIndex.get(customerKey).iterator().next();
//                                System.out.println("found customer " + customerObjId);
                            params.put("customerObjId_" + districtID, customerObjId);
                            ret.add(customerObjId);

                            String orderLineKey = Row.genSId("OrderLine", terminalWarehouseID, districtID, orderObjId.getSId().split(":")[4].split("=")[1]);
//                                System.out.println("finding ol keys " + orderLineKey);
                            if (secondaryIndex.get(orderLineKey) != null) {
                                Set<ObjId> orderLines = secondaryIndex.get(orderLineKey);
//                                    System.out.println("found orderLiness" + orderLines);
                                params.put("orderLineObjIds_" + districtID, orderLines);
                                ret.addAll(orderLines);
                            }
                        }
//                            if (!found)
//                                System.out.println("Found no new order for w=" + terminalWarehouseID + ":d=" + districtID);
                    }

                    break;
                }
                case STOCK_LEVEL: {
                    int terminalWarehouseID = (int) params.get("w_id");
                    int districtID = (int) params.get("d_id");
                    ObjId warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
                    ObjId districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
                    districtObjId.includeDependencies = true;
                    ret.add(warehouseObjId);
                    ret.add(districtObjId);
                    Set<ObjId> stockDistrictObjIds = TpccUtil.getStockDistrictId(terminalWarehouseID);
                    stockDistrictObjIds.forEach(objId -> objId.includeDependencies = true);
                    ret.addAll(stockDistrictObjIds);
//                        System.out.println("Getting those objs" + stockDistrictObjIds);
//                        ObjId stockDistrictObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, TpccConfig.defautDistrictForStock)).iterator().next();
//                        stockDistrictObjId.includeDependencies = true;
//                        ret.add(stockDistrictObjId);
                    break;
                }
            }
        } catch (Exception e) {
            System.out.println("P_" + this.partitionId + " ERROR: " + e.getMessage() + " command " + command.toFullString());
            e.printStackTrace();
//                throw e;
        }

        return ret;
    }

    @Override
    public Set<ObjId> extractObjectId(Command command) {

        command.rewind();
        if (command.getInvolvedObjects() != null && command.getInvolvedObjects().size() > 0) {
            return command.getInvolvedObjects();
        }
        Set<ObjId> ret = new HashSet<>();
        if (this.role.equals("CLIENT") || this.role.equals("ORACLE")) {
            if (command.getItem(0) instanceof TpccCommandType) {
                TpccCommandType cmdType = (TpccCommandType) command.getNext();
                Map<String, Object> params = (Map<String, Object>) command.getItem(1);
                switch (cmdType) {
                    case NEW_ORDER: {
                        int terminalWarehouseID = (int) params.get("w_id");
                        int districtID = (int) params.get("d_id");
                        ObjId districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
                        ObjId warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
                        ret.add(districtObjId);
                        ret.add(warehouseObjId);
                        break;
                    }
                    case PAYMENT: {
                        int terminalWarehouseID = (int) params.get("w_id");
                        int districtID = (int) params.get("d_id");
                        ObjId districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
                        ObjId warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
                        ret.add(districtObjId);
                        ret.add(warehouseObjId);
                        int c_w_id = (int) params.get("c_w_id");
                        int c_d_id = (int) params.get("c_d_id");
                        ObjId customerDistrictObjId = secondaryIndex.get(Row.genSId("District", c_w_id, c_d_id)).iterator().next();
                        ObjId customerWarehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", c_w_id)).iterator().next();
                        ret.add(customerDistrictObjId);
                        ret.add(customerWarehouseObjId);
                        break;
                    }
                    case ORDER_STATUS: {
                        int terminalWarehouseID = (int) params.get("w_id");
                        int districtID = (int) params.get("d_id");
                        ObjId districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
                        ObjId warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
                        ret.add(districtObjId);
                        ret.add(warehouseObjId);
                        break;
                    }
                    case DELIVERY: {
                        int terminalWarehouseID = (int) params.get("w_id");
                        ObjId warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
                        ret.add(warehouseObjId);
                        break;
                    }
                    case STOCK_LEVEL: {
                        int terminalWarehouseID = (int) params.get("w_id");
                        int districtID = (int) params.get("d_id");
                        ObjId districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
                        ObjId warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
                        ret.add(districtObjId);
                        ret.add(warehouseObjId);
                        break;
                    }
                    case READ: {
                        ObjId objId = secondaryIndex.get(params.get("key")).iterator().next();
                        ret.add(objId);
                        break;
                    }
                }
            } else ret = _extractObjId(command);
        } else if (this.role.equals("SERVER") && command.getItem(0) instanceof TpccCommandType) {
//            System.out.println("Extracting objects of command "+command);
            try {
                TpccCommandType cmdType = (TpccCommandType) command.getNext();
                Map<String, Object> params = (Map<String, Object>) command.getItem(1);

                switch (cmdType) {
                    case READ: {
                        ObjId key = secondaryIndex.get(params.get("key")).iterator().next();
                        ret.add(key);
                        break;
                    }
                    case NEW_ORDER: {
                        int terminalWarehouseID = (int) params.get("w_id");
                        int districtID = (int) params.get("d_id");
                        int customerID = (int) params.get("c_id");
                        int numItems = (int) params.get("ol_o_cnt");
                        int allLocal = (int) params.get("o_all_local");
                        int[] itemIDs = (int[]) params.get("itemIds");
                        int[] supplierWarehouseIDs = (int[]) params.get("supplierWarehouseIDs");
                        int[] orderQuantities = (int[]) params.get("orderQuantities");
//                        System.out.println("All local - "+allLocal+" - w_id="+terminalWarehouseID+" - d_id="+districtID+" - supplierWarehouseIDs="+supplierWarehouseIDs[0]);
                        ObjId warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
                        ObjId districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();

                        ObjId customerObjId = new ObjId(Row.genSId("Customer", terminalWarehouseID, districtID, customerID));
                        //TODO testing
//                        ObjId customerObjId = secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerID)).iterator().next();
                        Set<ObjId> itemObjIds = new HashSet<>();
                        for (int i = 0; i < numItems; i++) {
                            if (!secondaryIndex.containsKey(Row.genSId("Item", itemIDs[i]))) {
                                command.setInvalid(true);
                                return ret;
                            }
                            itemObjIds.add(secondaryIndex.get(Row.genSId("Item", itemIDs[i])).iterator().next());
                        }
                        Set<ObjId> supplierWarehouseObjIds = new HashSet<>();
                        for (int i = 0; i < numItems; i++) {
                            //TODO testing
                            supplierWarehouseObjIds.add(secondaryIndex.get(Row.genSId("Warehouse", supplierWarehouseIDs[i])).iterator().next());
//                            supplierWarehouseObjIds.add(new ObjId(Row.genSId("Warehouse", supplierWarehouseIDs[i])));
                        }

                        Set<ObjId> stockObjIds = new HashSet<>();
                        for (int i = 0; i < numItems; i++) {
                            //TODO testing
//                            stockObjIds.add(secondaryIndex.get(Row.genSId("Stock", supplierWarehouseIDs[i], itemIDs[i])).iterator().next());
                            stockObjIds.add(new ObjId(Row.genSId("Stock", supplierWarehouseIDs[i], itemIDs[i])));
                        }
                        ret.add(warehouseObjId);
                        ret.add(districtObjId);
                        ret.add(customerObjId);
                        ret.addAll(itemObjIds);
                        ret.addAll(supplierWarehouseObjIds);
                        ret.addAll(stockObjIds);
//                        mostRecentOrderdItem.addAll(stockObjIds);
                        break;
                    }
                    case PAYMENT: {
                        int terminalWarehouseID = (int) params.get("w_id");
                        int districtID = (int) params.get("d_id");
                        ObjId warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
                        ObjId districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
                        ret.add(warehouseObjId);
                        ret.add(districtObjId);

                        int customerWarehouseID = (int) params.get("c_w_id");
                        int customerDistrictID = (int) params.get("c_d_id");

                        ObjId customerWarehouseObjId = new ObjId(Row.genSId("Warehouse", terminalWarehouseID));
                        ObjId customerDistrictObjId = new ObjId(Row.genSId("District", terminalWarehouseID, districtID));
//                        ObjId customerWarehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
//                        ObjId customerDistrictObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
                        ret.add(customerWarehouseObjId);
                        ret.add(customerDistrictObjId);

                        boolean customerByName = (boolean) params.get("c_by_name");
                        if (!customerByName) {
                            int customerID = (int) params.get("c_id");

//                            ObjId customerObjId = new ObjId(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerID));
//                            logger.info("P_{} looking for customer {}", this.partitionId, Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerID));
                            ObjId customerObjId = secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerID)).iterator().next();
                            ret.add(customerObjId);
                        } else {
                            long seed = (long) params.get("randomSeed");
                            Random r = new Random(seed);
                            String customerLastName = TpccUtil.getLastName(r);
//                            ObjId customerObjId = new ObjId(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerID));
                            ObjId customerObjId = secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerLastName)) != null ?
                                    secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerLastName)).iterator().next() : null;
                            while (customerObjId == null) {
                                customerLastName = TpccUtil.getLastName(r);
//                            System.out.println("looking for " + customerLastName);
                                if (secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerLastName)) == null)
                                    continue;
                                customerObjId = secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerLastName)).iterator().next();
//                            System.out.println("found " + customerLastName + " - " + customerObjId);
                            }
                            params.put("c_objId", customerObjId);
                            ret.add(customerObjId);
                        }
                        break;
                    }
                    case ORDER_STATUS: {
                        int terminalWarehouseID = (int) params.get("w_id");
                        int districtID = (int) params.get("d_id");
                        ObjId warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
                        ObjId districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
                        ret.add(warehouseObjId);
                        ret.add(districtObjId);
                        boolean customerByName = (boolean) params.get("c_by_name");
                        ObjId customerObjId;
                        if (!customerByName) {
                            int customerID = (int) params.get("c_id");
                            customerObjId = secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerID)).iterator().next();
                            ret.add(customerObjId);
                        } else {
                            long seed = (long) params.get("randomSeed");
                            Random r = new Random(seed);
                            String customerLastName = TpccUtil.getLastName(r);
                            customerObjId = secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerLastName)) != null ?
                                    secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerLastName)).iterator().next() : null;
                            while (customerObjId == null) {
                                customerLastName = TpccUtil.getLastName(r);
//                            System.out.println("looking for "+customerLastName);
                                if (secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerLastName)) == null)
                                    continue;
                                customerObjId = secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerLastName)).iterator().next();
//                            System.out.println("found "+customerLastName+" - "+customerObjId);
                            }
                            params.put("c_objId", customerObjId);
                            ret.add(customerObjId);
                        }

//                    System.out.println("customer id " + customerObjId.getSId());
//                    System.out.println("extracted id: " + customerObjId.getSId().split(":")[3].split("=")[1]);
                        String ordersKey = Row.genSId("Order", terminalWarehouseID, districtID, customerObjId.getSId().split(":")[3].split("=")[1]);
//                    System.out.println("finding keys " + ordersKey);

                        if (secondaryIndex.get(ordersKey) != null) {
//                            List<ObjId> orders = new ArrayList<>();
//                            synchronized (secondaryIndex.get(ordersKey)) {
//                                orders.addAll(secondaryIndex.get(ordersKey));
//                            }
////                            orders.addAll(secondaryIndex.get(ordersKey));
//                            Collections.sort(orders, new OrderIdComparator());
//
//                        System.out.println("found orders" + orders);
                            ObjId orderObjId = new ObjId(ordersKey);
//                            ObjId orderObjId = secondaryIndex.get(ordersKey).iterator().next();
                            params.put("orderObjId", orderObjId);
                            ret.add(orderObjId);
                            String orderLineKey = Row.genSId("OrderLine", terminalWarehouseID, districtID, orderObjId.getSId().split(":")[4].split("=")[1]);
//                        System.out.println("finding ol keys " + orderLineKey);
                            if (secondaryIndex.get(orderLineKey) != null) {
                                Set<ObjId> orderLines = secondaryIndex.get(orderLineKey);
//                            System.out.println("found orderLiness" + orderLines);
                                params.put("orderLineObjIds", orderLines);
                                ret.addAll(orderLines);
                            }
                        }
                        break;
                    }
                    case DELIVERY: {
                        int terminalWarehouseID = (int) params.get("w_id");
                        ObjId warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
                        ret.add(warehouseObjId);
                        for (int districtID = 1; districtID <= TpccConfig.configDistPerWhse; districtID++) {
                            ObjId districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
                            ret.add(districtObjId);

                            String newOrdersKey = Row.genSId("NewOrder", terminalWarehouseID, districtID);
//                            System.out.println("finding newOrdersKey keys " + newOrdersKey);
                            boolean found = false;
                            if (secondaryIndex.get(newOrdersKey) != null) {
                                found = true;
//                                List<ObjId> newOrders = new ArrayList<>();
                                ObjId newOrderObjId;
//                                synchronized (secondaryIndex.get(newOrdersKey)) {
//                                    newOrders.addAll(secondaryIndex.get(newOrdersKey));
                                newOrderObjId = secondaryIndex.get(newOrdersKey).iterator().next();
//                                }
//                                System.out.println("found newOrders sorted" + newOrderObjId);
//                                Collections.sort(newOrders, new NewOrderIdComparator());
//                            System.out.println("found newOrders" + newOrders);
//                                newOrderObjId = newOrders.iterator().next();
                                params.put("newOrderObjId_" + districtID, newOrderObjId);
                                ret.add(newOrderObjId);


                                String orderIdKey = Row.genSId("Order", terminalWarehouseID, districtID, -1, newOrderObjId.getSId().split(":")[3].split("=")[1]);
//                            System.out.println("orderId key" + orderIdKey);

                                if (secondaryIndex.get(orderIdKey) == null) {
                                    System.out.println("ERROR: Can't find order " + orderIdKey);
                                    command.setInvalid(true);
                                    return ret;
                                }
                                ObjId orderObjId = secondaryIndex.get(orderIdKey).iterator().next();
//                                System.out.println("found order id" + orderObjId);
                                params.put("orderObjId_" + districtID, orderObjId);
                                ret.add(orderObjId);

                                String customerKey = Row.genSId("Customer", terminalWarehouseID, districtID, Integer.parseInt(orderObjId.getSId().split(":")[3].split("=")[1]));
//                                System.out.println("customerKey " + customerKey);
//                                if (!secondaryIndex.containsKey(customerKey))
//                                    System.out.println("w_id=" + warehouseObjId + "d_id=" + districtID + "-" + orderObjId + "-can't find customerKey " + customerKey);
                                ObjId customerObjId = secondaryIndex.get(customerKey).iterator().next();
//                                System.out.println("found customer " + customerObjId);
                                params.put("customerObjId_" + districtID, customerObjId);
                                ret.add(customerObjId);

                                String orderLineKey = Row.genSId("OrderLine", terminalWarehouseID, districtID, orderObjId.getSId().split(":")[4].split("=")[1]);
//                                System.out.println("finding ol keys " + orderLineKey);
                                if (secondaryIndex.get(orderLineKey) != null) {
                                    Set<ObjId> orderLines = secondaryIndex.get(orderLineKey);
//                                    System.out.println("found orderLiness" + orderLines);
                                    params.put("orderLineObjIds_" + districtID, orderLines);
                                    ret.addAll(orderLines);
                                }
                            }
//                            if (!found)
//                                System.out.println("Found no new order for w=" + terminalWarehouseID + ":d=" + districtID);
                        }

                        break;
                    }
                    case STOCK_LEVEL: {
                        int terminalWarehouseID = (int) params.get("w_id");
                        int districtID = (int) params.get("d_id");
                        ObjId warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
                        ObjId districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
                        districtObjId.includeDependencies = true;
                        ret.add(warehouseObjId);
                        ret.add(districtObjId);
                        Set<ObjId> stockDistrictObjIds = TpccUtil.getStockDistrictId(terminalWarehouseID);
                        stockDistrictObjIds.forEach(objId -> objId.includeDependencies = true);
                        ret.addAll(stockDistrictObjIds);
//                        System.out.println("Getting those objs" + stockDistrictObjIds);
//                        ObjId stockDistrictObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, TpccConfig.defautDistrictForStock)).iterator().next();
//                        stockDistrictObjId.includeDependencies = true;
//                        ret.add(stockDistrictObjId);
                        break;
                    }
                }
            } catch (Exception e) {
                System.out.println("P_" + this.partitionId + " ERROR: " + e.getMessage() + " command " + command.toFullString());
                e.printStackTrace();
//                throw e;
            }
        } else {
            ret = _extractObjId(command);
        }
        return ret;
    }

    private Set<ObjId> _extractObjId(Command command) {
        Set<ObjId> ret = new HashSet<>();
        if (command.getInvolvedObjects() != null && command.getInvolvedObjects().size() > 0)
            return command.getInvolvedObjects();
        if (StateMachine.isCreateWithGather(command)) {
            command.setInvolvedObjects((Set<ObjId>) command.getItem(2));
            return (Set<ObjId>) command.getItem(2);
        }
        command.rewind();
        while (command.hasNext()) {
            Object test = command.getNext();
            if (test instanceof ObjId) {
                ret.add((ObjId) test);
            } else if (test instanceof List) {
                for (Object objId : (List) test) {
                    if (objId instanceof ObjId) {
                        ret.add((ObjId) objId);
                    }
                }
            } else if (test instanceof Set) {
                for (Object objId : (Set) test) {
                    if (objId instanceof ObjId) {
                        ret.add((ObjId) objId);
                    }
                }
            } else if (test instanceof Map) {
                for (Object objId : ((Map) test).keySet()) {
                    if (objId instanceof ObjId) {
                        ret.add((ObjId) objId);
                    }
                }
                for (Object objId : ((Map) test).values()) {
                    if (objId instanceof ObjId) {
                        ret.add((ObjId) objId);
                    } else if (objId instanceof Set) {
                        for (Object o : (Set) objId) {
                            if (o instanceof ObjId) {
                                ret.add((ObjId) o);
                            }
                        }
                    }
                }
            } else if (test instanceof Command) {
                ((Command) test).rewind();
                ret.addAll(extractObjectId((Command) test));
            }
        }
        Set<ObjId> tmp = extractRelatedObjects(command);
        if (tmp != null) ret.addAll(tmp);
        return ret;
    }

    @Override
    public ObjId genParentObjectId(ObjId objId) {
        String[] idParts = objId.getSId().split(":");
        StringBuilder id = new StringBuilder("District:d_w_id=");
        switch (idParts[0]) {
            case "Customer": {
                id.append(idParts[1].split("=")[1]);
                id.append(":d_id=");
                id.append(idParts[2].split("=")[1]);
                break;
            }
            case "History": {
                id.append(idParts[1].split("=")[1]);
                id.append(":d_id=");
                id.append(idParts[2].split("=")[1]);
                break;
            }
            case "Stock": {
                id.append(idParts[1].split("=")[1]);
                id.append(":d_id=");
                id.append(TpccUtil.mapStockToDistrict(objId.getSId()));
                break;
            }
            case "Order": {
                id.append(idParts[1].split("=")[1]);
                id.append(":d_id=");
                id.append(idParts[2].split("=")[1]);
                break;
            }
            case "NewOrder": {
                id.append(idParts[1].split("=")[1]);
                id.append(":d_id=");
                id.append(idParts[2].split("=")[1]);
                break;
            }
            case "OrderLine": {
                id.append(idParts[1].split("=")[1]);
                id.append(":d_id=");
                id.append(idParts[2].split("=")[1]);
                break;
            }

        }
        return new ObjId(id.toString());
    }

    @Override
    public PRObjectNode getParentObject(PRObjectNode node) {
        String[] idParts = node.getId().getSId().split(":");
        StringBuilder id = new StringBuilder("District:d_w_id=");
        switch (idParts[0]) {
            case "Customer": {
                id.append(idParts[1].split("=")[1]);
                id.append(":d_id=");
                id.append(idParts[2].split("=")[1]);
                break;
            }
            case "History": {
                id.append(idParts[1].split("=")[1]);
                id.append(":d_id=");
                id.append(idParts[2].split("=")[1]);
                break;
            }
            case "Stock": {
                id.append(idParts[1].split("=")[1]);
                id.append(":d_id=");
                id.append(TpccUtil.mapStockToDistrict(node.getId().getSId()));
                break;
            }
            case "Order": {
                id.append(idParts[1].split("=")[1]);
                id.append(":d_id=");
                id.append(idParts[2].split("=")[1]);
                break;
            }
            case "NewOrder": {
                id.append(idParts[1].split("=")[1]);
                id.append(":d_id=");
                id.append(idParts[2].split("=")[1]);
                break;
            }
            case "OrderLine": {
                id.append(idParts[1].split("=")[1]);
                id.append(":d_id=");
                id.append(idParts[2].split("=")[1]);
                break;
            }

        }
        ObjId parentId;
        try {
            parentId = this.secondaryIndex.get(id.toString()).iterator().next();
        } catch (NullPointerException e) {
            System.out.println("ERROR: can't find parent of " + node + " - generated id=" + id.toString());
            throw e;
        }
        return this.graph.getNode(parentId);

    }

    @Override
    public boolean isTransient(ObjId objId) {
        if (objId.getSId() == null) return false;
        String modelName = splitterCollon.split(objId.getSId()).iterator().next();
        if (modelName == null) return false;
        if (modelName.equals("Customer")
                || modelName.equals("History")
                || modelName.equals("NewOrder")
                || modelName.equals("Order")
                || modelName.equals("OrderLine")
                || modelName.equals("Stock"))
            return true;
        return false;
    }

    @Override
    public void addToSecondaryIndex(Map<String, Set<ObjId>> secondaryIndex, String objKey, ObjId objId, PRObject obj) {
        List<String> keys = new ArrayList<>();
        if (obj != null) {
            keys.add(objKey);
            if (obj instanceof Warehouse) {
//                keys.add(safeKey("Warehouse", "w_id", String.valueOf(((Warehouse) obj).w_id)));
            } else if (obj instanceof District) {
//                keys.add(safeKey("District", "d_w_id", String.valueOf(((District) obj).d_w_id), "d_id", String.valueOf(((District) obj).d_id)));
            } else if (obj instanceof Customer) {
                keys.add(safeKey("Customer", "c_w_id", String.valueOf(((Customer) obj).c_w_id), "c_d_id", String.valueOf(((Customer) obj).c_d_id), "c_last", String.valueOf(((Customer) obj).c_last)));
            } else if (obj instanceof History) {

            } else if (obj instanceof Stock) {
            } else if (obj instanceof Order) {
                keys.add(safeKey("Order", "o_w_id", String.valueOf(((Order) obj).o_w_id), "o_d_id", String.valueOf(((Order) obj).o_d_id), "o_c_id", String.valueOf(((Order) obj).o_c_id)));
                keys.add(safeKey("Order", "o_w_id", String.valueOf(((Order) obj).o_w_id), "o_d_id", String.valueOf(((Order) obj).o_d_id), "o_id", String.valueOf(((Order) obj).o_id)));
            } else if (obj instanceof NewOrder) {
                keys.add(safeKey("NewOrder", "no_w_id", String.valueOf(((NewOrder) obj).no_w_id), "no_d_id", String.valueOf(((NewOrder) obj).no_d_id)));
            } else if (obj instanceof OrderLine) {
                keys.add(safeKey("OrderLine", "ol_w_id", String.valueOf(((OrderLine) obj).ol_w_id), "ol_d_id", String.valueOf(((OrderLine) obj).ol_d_id)));
                keys.add(safeKey("OrderLine", "ol_w_id", String.valueOf(((OrderLine) obj).ol_w_id), "ol_d_id", String.valueOf(((OrderLine) obj).ol_d_id), "ol_o_id", String.valueOf(((OrderLine) obj).ol_o_id)));
            } else if (obj instanceof Item) {
            }
        } else {
            keys.add(objKey);
        }
        for (String key : keys) {
            Set<ObjId> oids = secondaryIndex.get(key);
//            System.out.println("partition " + StateMachine.getMachine().getReplicaId() + " adding " + key + " - objid" + objId);
            if (oids == null) {
                String model = key.split(":")[0];
                if (model.equals("NewOrder")) {
                    Comparator comparator = new TpccProcedure.NewOrderIdComparator();
                    oids = new TreeSet(comparator);
                } else if (model.equals("OrderLine")) {
                    Comparator comparator = new TpccProcedure.OrderLineOrderIdComparator();
                    oids = new TreeSet(comparator);
                } else if (model.equals("Order")) {
                    Comparator comparator = new TpccProcedure.OrderIdComparator();
                    oids = new TreeSet(comparator);
                } else {
                    oids = new HashSet<>();
                }
                secondaryIndex.put(key, oids);
            }
            oids.add(objId);
        }
    }

    public static class OrderIdComparator implements Comparator<ObjId> {
        public int compare(ObjId c1, ObjId c2) {
//            return Integer.parseInt(Lists.newArrayList(splitterEqual.split(Lists.newArrayList(splitterCollon.split(c2.getSId())).get(4))).get(1)) - Integer.parseInt(Lists.newArrayList(splitterEqual.split(Lists.newArrayList(splitterCollon.split(c1.getSId())).get(4))).get(1));
            if (c2.sId.split(":").length < 5) System.out.print("AAAA" + c2);
            return Integer.parseInt(c2.getSId().split(":")[4].split("=")[1]) - Integer.parseInt(c1.getSId().split(":")[4].split("=")[1]);
        }
    }

    public static class OrderLineOrderIdComparator implements Comparator<ObjId> {
        public int compare(ObjId c1, ObjId c2) {
//            Iterator tmp1 = splitterCollon.split(c2.getSId()).iterator();
//            tmp1.next();
//            tmp1.next();
//            tmp1.next();
//            Iterator tmp2 = splitterEqual.split((CharSequence) tmp1.next()).iterator();
//            tmp2.next();
//            int i2 = Integer.parseInt((String) tmp2.next());
//
//            tmp1 = splitterCollon.split(c1.getSId()).iterator();
//            tmp1.next();
//            tmp1.next();
//            tmp1.next();
//            tmp2 = splitterEqual.split((CharSequence) tmp1.next()).iterator();
//            tmp2.next();
//            int i1 = Integer.parseInt((String) tmp2.next());
//            return i2 - i1;
            return Integer.parseInt(c2.getSId().split(":")[3].split("=")[1]) - Integer.parseInt(c1.getSId().split(":")[3].split("=")[1]);
        }
    }

    public static class NewOrderIdComparator implements Comparator<ObjId> {
        public int compare(ObjId c1, ObjId c2) {
//            return Integer.parseInt(Lists.newArrayList(splitterEqual.split(Lists.newArrayList(splitterCollon.split(c1.getSId())).get(3))).get(1)) - Integer.parseInt(Lists.newArrayList(splitterEqual.split(Lists.newArrayList(splitterCollon.split(c2.getSId())).get(3))).get(1));
            return Integer.parseInt(c1.getSId().split(":")[3].split("=")[1]) - Integer.parseInt(c2.getSId().split(":")[3].split("=")[1]);
        }
    }
}

