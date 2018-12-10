package ch.usi.dslab.lel.dynastar.tpcc.tables;

import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
import ch.usi.dslab.lel.dynastarv2.probject.PRObject;
import org.json.simple.JSONObject;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.lang.reflect.Field;
import java.util.*;

public abstract class Row extends PRObject {
    private static ScriptEngineManager mgr = new ScriptEngineManager();
    private static ScriptEngine engine = mgr.getEngineByName("JavaScript");
    private String strObjId;

    static int BASE_W_ID = 100000000;
    static int BASE_D_ID = 1000000;

    static int BASE_C_ID = 0;
    static int BASE_NO_ID = 10000;
    static int BASE_ITEM_ID = 100000;
    static int BASE_STOCK_ID = 200000;
    static int BASE_HIS_ID = 400000;
    static int BASE_ORDER_ID = 500000;
    static int BASE_ORDER_LINE_ID = 600000;


    public static ObjId genObjId(String model, Object... attr) {
        long value = -1;
        switch (model) {
            case "Warehouse": {
                value = Long.parseLong(attr[0].toString()) * BASE_W_ID;
                break;
            }
            case "District": {
                value = Long.parseLong(attr[0].toString()) * BASE_W_ID + Long.parseLong(attr[1].toString()) * BASE_D_ID;
                break;
            }
            case "Customer": {
                value = Long.parseLong(attr[0].toString()) * BASE_W_ID + Long.parseLong(attr[1].toString()) * BASE_D_ID + BASE_C_ID + Long.parseLong(attr[2].toString());
                break;
            }
            case "NewOrder": {
                value = Long.parseLong(attr[0].toString()) * BASE_W_ID + Long.parseLong(attr[1].toString()) * BASE_D_ID + BASE_NO_ID + Long.parseLong(attr[2].toString());
                break;
            }
            case "Item": {
                value = 1 * BASE_W_ID + 1 * BASE_D_ID + BASE_ITEM_ID + Long.parseLong(attr[0].toString());
                break;
            }
            case "Stock": {
                value = Long.parseLong(attr[0].toString()) * BASE_W_ID + 1 * BASE_D_ID + BASE_STOCK_ID + Long.parseLong(attr[1].toString());
                break;
            }
            case "History": {
                value = Long.parseLong(attr[0].toString()) * BASE_W_ID + Long.parseLong(attr[1].toString()) * BASE_D_ID + BASE_HIS_ID + Long.parseLong(attr[2].toString());
                break;
            }
            case "Order": {
                value = Long.parseLong(attr[0].toString()) * BASE_W_ID + Long.parseLong(attr[1].toString()) * BASE_D_ID + BASE_ORDER_ID + Long.parseLong(attr[2].toString()) * 10 + Long.parseLong(attr[3].toString());
                break;
            }
            case "OrderLine": {
                value = Long.parseLong(attr[0].toString()) * BASE_W_ID + Long.parseLong(attr[1].toString()) * BASE_D_ID + BASE_ORDER_LINE_ID + Long.parseLong(attr[2].toString()) * 1000 + Long.parseLong(attr[3].toString());
                break;
            }
        }
        return new ObjId(value);
    }

    public static String genSId(String model, Object... attr) {
        StringBuilder ret = new StringBuilder(model);

        switch (model) {
            case "Warehouse": {
                ret.append(":w_id=");
                ret.append(attr[0]);
                break;
            }
            case "District": {
                ret.append(":d_w_id=");
                ret.append(attr[0]);
                ret.append(":d_id=");
                ret.append(attr[1]);
                break;
            }
            case "Customer": {
                ret.append(":c_w_id=");
                ret.append(attr[0]);
                ret.append(":c_d_id=");
                ret.append(attr[1]);
                if (attr[2] instanceof String) {
                    ret.append(":c_last=");
                    ret.append(attr[2]);
                } else {
                    ret.append(":c_id=");
                    ret.append(attr[2]);
                }
                break;
            }
            case "History": {
                ret.append(":h_c_w_id=");
                ret.append(attr[0]);
                ret.append(":h_c_d_id=");
                ret.append(attr[1]);
                ret.append(":h_c_id=");
                ret.append(attr[2]);
                break;
            }
            case "Stock": {
                ret.append(":s_w_id=");
                ret.append(attr[0]);
                ret.append(":s_i_id=");
                ret.append(attr[1]);
                break;
            }
            case "Order": {
                //Order:o_w_id=x:o_d_id=y:o_c_id=z:o_id=t    or
                //Order:o_w_id=x:o_d_id=y:o_id=t  if z < 0
                ret.append(":o_w_id=");
                ret.append(attr[0]);
                ret.append(":o_d_id=");
                ret.append(attr[1]);
                if (attr.length >= 3) {
                    if ((int) attr[2] > 0) {
                        ret.append(":o_c_id=");
                        ret.append(attr[2]);
                    }
                    if (attr.length > 3) {
                        ret.append(":o_id=");
                        ret.append(attr[3]);
                    }
                }
                break;
            }
            case "NewOrder": {
                ret.append(":no_w_id=");
                ret.append(attr[0]);
                ret.append(":no_d_id=");
                ret.append(attr[1]);
                if (attr.length > 2) {
                    ret.append(":no_o_id=");
                    ret.append(attr[2]);
                }
                break;
            }
            case "OrderLine": {
                ret.append(":ol_w_id=");
                ret.append(attr[0]);
                ret.append(":ol_d_id=");
                ret.append(attr[1]);
                if (attr.length > 2) {
                    ret.append(":ol_o_id=");
                    ret.append(attr[2]);
                }
                if (attr.length > 3) {
                    ret.append(":ol_number=");
                    ret.append(attr[3]);
                }
                break;
            }
            case "Item": {
                ret.append(":i_id=");
                ret.append(attr[0]);
                break;
            }
        }
        return ret.toString();
    }

    public static String safeKey(Map<String, Object> obj, String model, String... keys) {
        StringBuilder ret = new StringBuilder(model);
        for (String key : keys) {
            ret.append(":");
            ret.append(key);
            ret.append("=");
            ret.append(obj.get(key));
        }
        return ret.toString();
    }

    public static String genSId(Map<String, Object> obj) {
        String model = (String) obj.get("model");
        switch (model) {
            case "Warehouse": {
                return safeKey(obj, model, "w_id");
            }
            case "District": {
                return safeKey(obj, model, "d_w_id", "d_id");
            }
            case "Customer": {
                return safeKey(obj, model, "c_w_id", "c_d_id", "c_id");
            }
            case "History": {
                return safeKey(obj, model, "h_c_w_id", "h_c_d_id", "h_c_id");
            }
            case "Stock": {
                return safeKey(obj, model, "s_w_id", "s_i_id");
            }
            case "Order": {
                return safeKey(obj, model, "o_w_id", "o_d_id", "o_c_id", "o_id");
            }
            case "NewOrder": {
                return safeKey(obj, model, "no_w_id", "no_d_id", "no_o_id");
            }
            case "OrderLine": {
                return safeKey(obj, model, "ol_w_id", "ol_d_id", "ol_o_id", "ol_number");
            }
            case "Item": {
                return safeKey(obj, model, "i_id");
            }
            default:
                return null;
        }
    }

    public static List<String> genStrObjId(Map<String, Object> obj) {
        String model = (String) obj.get("model");
        List<String> ret = new ArrayList<>();
        switch (model) {
            case "Warehouse": {
                ret.add(safeKey(obj, model, "w_id"));
                break;
            }
            case "District": {
                ret.add(safeKey(obj, model, "d_w_id", "d_id"));
                break;
            }
            case "Customer": {
                ret.add(safeKey(obj, model, "c_w_id", "c_d_id", "c_id"));
                ret.add(safeKey(obj, model, "c_w_id", "c_d_id", "c_last"));
                break;
            }
            case "History": {
                ret.add(safeKey(obj, model, "h_c_w_id", "h_c_d_id", "h_c_id"));
                break;
            }
            case "Stock": {
                ret.add(safeKey(obj, model, "s_w_id", "s_i_id"));
                break;
            }
            case "Order": {
                ret.add(safeKey(obj, model, "o_w_id", "o_d_id", "o_c_id", "o_id"));
                ret.add(safeKey(obj, model, "o_w_id", "o_d_id", "o_c_id"));
                ret.add(safeKey(obj, model, "o_w_id", "o_d_id", "o_id"));
                break;
            }
            case "NewOrder": {
                ret.add(safeKey(obj, model, "no_w_id", "no_d_id", "no_o_id"));
                ret.add(safeKey(obj, model, "no_w_id", "no_d_id"));
                break;
            }
            case "OrderLine": {
                ret.add(safeKey(obj, model, "ol_w_id", "ol_d_id", "ol_o_id", "ol_number"));
                ret.add(safeKey(obj, model, "ol_w_id", "ol_d_id", "ol_o_id"));
                ret.add(safeKey(obj, model, "ol_w_id", "ol_d_id"));
                break;
            }
            case "Item": {
                ret.add(safeKey(obj, model, "i_id"));
                break;
            }
        }
        return ret;
    }

    public static Map<String, Object> csvToHashMap(String[] headers, String[] values) {
        Map<String, Object> ret = new HashMap<>();
        for (int i = 1; i < headers.length; i++) {
            if (headers[i].equals("id") || headers[i].equals("model")) continue;
            ret.put(headers[i], values[i]);
        }
        return ret;
    }

    public static Object interpret(String s) {
        if (s.matches(".*[a-df-zA-DF-Z|]+.*")) return s;
        Scanner sc = new Scanner(s);
        return sc.hasNextInt() ? sc.nextInt() :
                sc.hasNextLong() ? sc.nextLong() :
                        sc.hasNextDouble() ? sc.nextDouble() :
                                sc.hasNext() ? sc.next() :
                                        s;
    }

    public static boolean set(Object object, String fieldName, Object fieldValue) {
        Class<?> clazz = object.getClass();
        Field field = null;
        Class fieldType = null;
        while (clazz != null) {
            try {
                field = clazz.getDeclaredField(fieldName);
                fieldType = interpret(String.valueOf(fieldValue)).getClass();
                field.setAccessible(true);
                if ((fieldType == java.lang.Integer.class || fieldType == java.lang.Long.class || fieldType == java.lang.Double.class)
                        && (String.valueOf(fieldValue).matches("^[+-/\\*]+.*"))) {
                    String value = String.valueOf(engine.eval(String.valueOf(field.get(object)) + fieldValue));
                    field.set(object, fieldType.cast(interpret(value)));
                } else {
                    field.set(object, fieldType.cast(interpret(String.valueOf(fieldValue))));
                }
                return true;
            } catch (IllegalAccessException | ScriptException | NoSuchFieldException | IllegalArgumentException e) {
                System.out.println(field.getName());
                System.out.println(field.getName() + " - " + fieldType.getName() + " - " + fieldValue + " - " + interpret(String.valueOf(fieldValue)).toString());
                e.printStackTrace();
            }
        }
        return false;
    }

    public static MODEL getModelFromName(String name) {
        for (MODEL model : MODEL.values()) {
            if (model.getName().equals(name)) {
                return model;
            }
        }
        return null;
    }

    public static Class<?>[] getConstructorParams() {
        Class<?>[] ret = new Class<?>[1];
        ret[0] = ObjId.class;
        return ret;
    }

    public static ObjId genObjId(String sId) {
        return new ObjId(sId);
    }

    @Override
    public void setId(ObjId id) {
        id.setSId(this.getObjIdString());
        super.setId(id);
    }

    public void setStrObjId() {
        this.strObjId = this.getObjIdString();
    }

    public abstract String getObjIdString();

    public abstract String[] getPrimaryKeys();

    public abstract String getModelName();

    public Map<String, Object> toHashMap() {
        Map<String, Object> ret = new HashMap<>();
        Field[] fields = this.getClass().getFields();
        try {
            for (Field f : fields) {
                if (!f.getName().equals("model") && !f.getName().equals("id"))
                    ret.put(f.getName(), f.get(this));
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        ret.put("model", this.getModelName());
        return ret;
    }

    public String toCSVString() {
        List<String> ret = new ArrayList<>();
        ret.add(this.getModelName());
        ret.add(String.valueOf(this.getId().value));
        Map<String, Object> attrs = this.toHashMap();
        String[] keys = this.getCSVHeader() == null ? (String[]) attrs.keySet().toArray() : this.getCSVHeader().split(",");
        for (String key : keys) {
            ret.add(String.valueOf(attrs.get(key)));
        }

        return String.join(",", ret);
    }

    public abstract String getCSVHeader();

    public String toCSVHeader() {
        List<String> ret = new ArrayList<>();
        ret.add("Header");
        ret.add(this.getModelName());
        ret.add("ObjId");
        if (this.getCSVHeader() != null) return String.join(",", ret) + "," + this.getCSVHeader();
        Map<String, Object> attrs = this.toHashMap();
        for (String key : attrs.keySet()) {
            ret.add(String.valueOf(key));
        }
        return String.join(",", ret);
    }

    public JSONObject toJSON() {
        JSONObject ret = new JSONObject();
        Map<String, Object> attrs = this.toHashMap();
        for (String key : attrs.keySet()) {
            ret.put(key, attrs.get(key));
        }
        ret.put("objId", this.getObjIdString());
        return ret;
    }

    public <T extends Row> T fromCSV(String[] headers, String[] values) {
        for (int i = 1; i < headers.length; i++) {
            if (headers[i].equals("id")) {
                try {
                    Class<?> clazz = this.getClass();
                    Field field = clazz.getField("id");
                    field.set(this, new ObjId(Integer.parseInt(values[i])));
                    continue;
                } catch (IllegalAccessException | NoSuchFieldException e) {
                    e.printStackTrace();
                }
            }
            if (values[i] != null) {
                set(this, headers[i], values[i]);
            }
        }
        return (T) this;
    }

    public enum MODEL {
        WAREHOUSE(1, "Warehouse"), DISTRICT(2, "District"), CUSTOMER(3, "Customer"), HISTORY(4, "History"),
        NEWORDER(5, "NewOrder"), ORDER(6, "Order"), ORDERLINE(7, "OrderLine"), ITEM(8, "Item"), STOCK(9, "Stock"), RECENT_PURCHASED_ITEM(10, "RecentPurchasedItem");
        int value;
        String name;

        MODEL(int value, String name) {
            this.value = value;
            this.name = name;
        }

        public int getValue() {
            return this.value;
        }

        public String getName() {
            return this.name;
        }

    }
}
