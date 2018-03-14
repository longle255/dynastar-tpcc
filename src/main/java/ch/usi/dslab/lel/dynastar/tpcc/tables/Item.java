package ch.usi.dslab.lel.dynastar.tpcc.tables;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
import ch.usi.dslab.lel.dynastarv2.probject.PRObject;


public class Item extends Row {

    public final MODEL model = MODEL.ITEM;
    public int i_id; // PRIMARY KEY
    public int i_im_id;
    public double i_price;
    public String i_name;
    public String i_data;

    public Item() {

    }

    public Item(ObjId id) {
        this.setId(id);
    }

    public Item(int i_id) {
        this.i_id = i_id;
        this.setStrObjId();
    }


    @Override
    public void updateFromDiff(Message objectDiff) {
        this.i_im_id = (int) objectDiff.getNext();
        this.i_price = (double) objectDiff.getNext();
        this.i_name = (String) objectDiff.getNext();
        this.i_data = (String) objectDiff.getNext();
    }

    @Override
    public Message getSuperDiff() {
         return new Message(this.i_im_id, this.i_price, this.i_name, this.i_data);
    }

    public String toString() {
        return (
                "\n***************** Item ********************" +
                        "\n*    i_id = " + i_id +
                        "\n*  i_name = " + i_name +
                        "\n* i_price = " + i_price +
                        "\n*  i_data = " + i_data +
                        "\n* i_im_id = " + i_im_id +
                        "\n**********************************************"
        );
    }

    @Override
    public String getObjIdString() {
        return model.getName() + ":i_id=" + i_id;
    }

    @Override
    public String[] getPrimaryKeys() {
        return new String[]{"i_id"};
    }

    @Override
    public String getModelName() {
        return this.model.getName();
    }
}  // end Item