package ch.usi.dslab.lel.dynastar.tpcc.tpcc;/*
 * jTPCCUtil - utility functions for the Open Source Java implementation of
 *    the TPC-C benchmark
 *
 * Copyright (C) 2003, Raul Barbosa
 * Copyright (C) 2004-2016, Denis Lussier
 *
 */


import ch.usi.dslab.lel.dynastar.tpcc.TpccCommandType;
import ch.usi.dslab.lel.dynastar.tpcc.TpccProcedure;
import ch.usi.dslab.lel.dynastar.tpcc.tables.Row;
import ch.usi.dslab.lel.dynastarv2.Partition;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
import ch.usi.dslab.lel.dynastarv2.probject.PRObjectGraph;

import java.util.*;

public class TpccUtil extends TpccConfig {

    public static String getSysProp(String inSysProperty, String defaultValue) {

        String outPropertyValue = null;

        try {
            outPropertyValue = System.getProperty(inSysProperty, defaultValue);
        } catch (Exception e) {
            System.err.println("Error Reading Required System Property '" + inSysProperty + "'");
        }

        return (outPropertyValue);

    } // end getSysProp


    public static String randomStr(long strLen) {

        char freshChar;
        String freshString;
        freshString = "";

        while (freshString.length() < (strLen - 1)) {

            freshChar = (char) (Math.random() * 128);
            if (Character.isLetter(freshChar)) {
                freshString += freshChar;
            }
        }

        return (freshString);

    } // end randomStr


    public static String getCurrentTime() {
        return dateFormat.format(new java.util.Date());
    }

    public static String formattedDouble(double d) {
        String dS = "" + d;
        return dS.length() > 6 ? dS.substring(0, 6) : dS;
    }

    public static int getItemID(Random r) {
//        return nonUniformRandom(8191, 1, 100000, r);
        return nonUniformRandom(((Long) Math.round(TpccConfig.configItemCount * 0.08191)).intValue(), 1, TpccConfig.configItemCount, r);
    }

    public static int getCustomerID(Random r) {
//        return nonUniformRandom(1023, 1, 3000, r);
        return nonUniformRandom(((Long) Math.round(TpccConfig.configCustPerDist * 0.341)).intValue(), 1, TpccConfig.configCustPerDist, r);
    }

    public static String getLastName(Random r) {
        int num = (int) nonUniformRandom(255, 0, 999, r);
        return nameTokens[num / 100] + nameTokens[(num / 10) % 10] + nameTokens[num % 10];
    }

    public static int randomNumber(int min, int max, Random r) {
        return (int) (r.nextDouble() * (max - min + 1) + min);
    }


    // A: A is a constant chosen according to the size of the range [x .. y] for C_LAST, the range is [0 .. 999] and A = 255
    //    for C_ID, the range is [1 .. 3000] and A = 1023
    //    for OL_I_ID, the range is [1 .. 100000] and A = 8191
    public static int nonUniformRandom(int A, int min, int max, Random r) {
        return (((randomNumber(0, A, r) | randomNumber(min, max, r)) + randomNumber(0, A, r)) % (max - min + 1)) + min;
    }

    public static String leftPad(String str, int left) {
        return org.apache.commons.lang.StringUtils.leftPad(str, left, " ");
    }


    public static int mapIdToPartition(ObjId objId) {
        if (objId.getSId() == null) return objId.hashCode() % Partition.getPartitionList().size();
        String parts[] = objId.getSId().split(":");
        switch (parts[0]) {
            case "Warehouse":
                return Integer.parseInt(parts[1].split("=")[1]) % Partition.getPartitionList().size();
            case "Stock":
//                return TpccConfig.defautDistrictForStock % Partition.getPartitionList().size();
                return mapStockToDistrict(objId.sId) % Partition.getPartitionList().size();
            case "District":
            case "Customer":
            case "History":
            case "Order":
            case "NewOrder":
            case "OrderLine":
                return Integer.parseInt(parts[2].split("=")[1]) % Partition.getPartitionList().size();
            case "Item":
                return objId.hashCode() % Partition.getPartitionList().size();
            default:
                return 0;
        }

    }


    public static void loadDataToCache(String dataFile, PRObjectGraph graph, Map<String, Set<ObjId>> secondaryIndex, Callback callback) {
        Map<String, String[]> ret = new HashMap<>();
        TpccDataGenerator.loadCSVData(dataFile, line -> {
            String[] tmp = line.split(",");
            ObjId objId = null;
            Map<String, Object> obj = null;
            if (tmp.length == 2) {

            } else {
                switch (tmp[0]) {
                    case "Header":
                        ret.put(tmp[1], Arrays.copyOfRange(tmp, 1, tmp.length));
                        break;
                    default:
                        objId = new ObjId(Integer.parseInt(tmp[1]));
                        obj = Row.csvToHashMap(ret.get(tmp[0]), tmp);
                        break;
                }
            }
            if (objId != null) {
                obj.put("model", tmp[0]);
                objId.setSId(Row.genSId(obj));
                List<String> keys = Row.genStrObjId(obj);
                for (String key : keys) {
                    new TpccProcedure().addToSecondaryIndex(secondaryIndex, key, objId, null);
                }
                if (callback != null) callback.callback(objId, obj);
            }
        });
    }

    public static void extractObjectIds(TpccCommandType cmdType, Map<String, Object> params, PRObjectGraph graph, Map<String, Set<ObjId>> secondaryIndex) {
        switch ((TpccCommandType) cmdType) {
            case NEW_ORDER: {
                int terminalWarehouseID = (int) params.get("w_id");
                int districtID = (int) params.get("d_id");
                int customerID = (int) params.get("c_id");
                int numItems = (int) params.get("ol_o_cnt");
                int allLocal = (int) params.get("o_all_local");
                int[] itemIDs = (int[]) params.get("itemIds");
                int[] supplierWarehouseIDs = (int[]) params.get("supplierWarehouseIDs");
                int[] orderQuantities = (int[]) params.get("orderQuantities");
                ObjId warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
                ObjId districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
                ObjId customerDistrictId = secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerID)).iterator().next();
                Set<ObjId> itemObjIds = new HashSet<>();
                for (int i = 0; i < numItems; i++) {
                    itemObjIds.add(secondaryIndex.get(Row.genSId("Item", itemIDs[i])).iterator().next());
                }
                Set<ObjId> supplierWarehouseObjIds = new HashSet<>();
                for (int i = 0; i < numItems; i++) {
                    supplierWarehouseObjIds.add(secondaryIndex.get(Row.genSId("Warehouse", supplierWarehouseIDs[i])).iterator().next());
                }

                Set<ObjId> stockObjIds = new HashSet<>();
                for (int i = 0; i < numItems; i++) {
                    stockObjIds.add(secondaryIndex.get(Row.genSId("Stock", supplierWarehouseIDs[i], itemIDs[i])).iterator().next());
                }

                break;
            }
        }
    }

    public static int mapStockToDistrict(String sId) {
        String parts[] = sId.split(":");
        if (!parts[0].equals("Stock")) {
            System.out.println(sId + "is not a stock level");
            System.exit(-1);
        }
        int i_id = Integer.parseInt(parts[2].split("=")[1]);
        return i_id % TpccConfig.configDistPerWhse + 1;
    }

    public static Set<ObjId> getStockDistrictId(int w_id) {
        Set<ObjId> ret = new HashSet<>();
        for (int i = 1; i <= TpccConfig.configDistPerWhse; i++) {
            ret.add(new ObjId(Row.genSId("District", w_id, i)));
        }
        return ret;
    }

    public interface Callback {
        void callback(ObjId objId, Map<String, Object> obj);
    }
} // end jTPCCUtil
