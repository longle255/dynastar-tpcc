package ch.usi.dslab.lel.dynastar.tpcc.tables;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
import ch.usi.dslab.lel.dynastarv2.probject.PRObject;


public class Stock extends Row {
    public final MODEL model = MODEL.STOCK;
    public int s_w_id;  //PRIMARY KEY 1
    public int s_i_id;  //PRIMARY KEY 2
    public int s_order_cnt;
    public int s_remote_cnt;
    public int s_quantity;
    public double s_ytd;
    public String s_data;
    public String s_dist_01;
    public String s_dist_02;
    public String s_dist_03;
    public String s_dist_04;
    public String s_dist_05;
    public String s_dist_06;
    public String s_dist_07;
    public String s_dist_08;
    public String s_dist_09;
    public String s_dist_10;

    public Stock() {

    }

    public Stock(ObjId id) {
        this.setId(id);
    }

    public Stock(int s_w_id, int s_i_id) {
        this.s_w_id = s_w_id;
        this.s_i_id = s_i_id;
        this.setStrObjId();
    }

    @Override
    public void updateFromDiff(Message objectDiff) {
        this.s_order_cnt = (int) objectDiff.getNext();
        this.s_remote_cnt = (int) objectDiff.getNext();
        this.s_quantity = (int) objectDiff.getNext();
        this.s_ytd = (double) objectDiff.getNext();
        this.s_data = (String) objectDiff.getNext();
        this.s_dist_01 = (String) objectDiff.getNext();
        this.s_dist_02 = (String) objectDiff.getNext();
        this.s_dist_03 = (String) objectDiff.getNext();
        this.s_dist_04 = (String) objectDiff.getNext();
        this.s_dist_05 = (String) objectDiff.getNext();
        this.s_dist_06 = (String) objectDiff.getNext();
        this.s_dist_07 = (String) objectDiff.getNext();
        this.s_dist_08 = (String) objectDiff.getNext();
        this.s_dist_09 = (String) objectDiff.getNext();
        this.s_dist_10 = (String) objectDiff.getNext();
    }


    @Override
    public Message getSuperDiff() {
        return new Message(this.s_order_cnt, this.s_remote_cnt, this.s_quantity, this.s_ytd, this.s_data, this.s_dist_01, this.s_dist_02, this.s_dist_03, this.s_dist_04, this.s_dist_05, this.s_dist_06, this.s_dist_07, this.s_dist_08, this.s_dist_09, this.s_dist_10);
    }

    public String toString() {
        return (

                "\n***************** Stock ********************" +
                        "\n*       s_i_id = " + s_i_id +
                        "\n*       s_w_id = " + s_w_id +
                        "\n*   s_quantity = " + s_quantity +
                        "\n*        s_ytd = " + s_ytd +
                        "\n*  s_order_cnt = " + s_order_cnt +
                        "\n* s_remote_cnt = " + s_remote_cnt +
                        "\n*       s_data = " + s_data +
                        "\n*    s_dist_01 = " + s_dist_01 +
                        "\n*    s_dist_02 = " + s_dist_02 +
                        "\n*    s_dist_03 = " + s_dist_03 +
                        "\n*    s_dist_04 = " + s_dist_04 +
                        "\n*    s_dist_05 = " + s_dist_05 +
                        "\n*    s_dist_06 = " + s_dist_06 +
                        "\n*    s_dist_07 = " + s_dist_07 +
                        "\n*    s_dist_08 = " + s_dist_08 +
                        "\n*    s_dist_09 = " + s_dist_09 +
                        "\n*    s_dist_10 = " + s_dist_10 +
                        "\n**********************************************"
        );
    }

    @Override
    public String getObjIdString() {
        return model.getName() + ":s_w_id=" + s_w_id + ":s_i_id=" + s_i_id;
    }

    @Override
    public String[] getPrimaryKeys() {
        return new String[]{"s_w_id", "s_i_id"};
    }

    @Override
    public String getModelName() {
        return this.model.getName();
    }

}  // end Stock