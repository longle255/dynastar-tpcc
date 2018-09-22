/*
 * Nest - A library for developing DSSMR-based services
 * Copyright (C) 2015, University of Lugano
 *
 *  This file is part of Nest.
 *
 *  Nest is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */

package ch.usi.dslab.lel.dynastar.tpcc.tables;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
import ch.usi.dslab.lel.dynastarv2.probject.PRObject;

import java.util.List;


/**
 * Created by longle on 18/02/16.
 */
public class Customer extends Row {
    public final MODEL model = MODEL.CUSTOMER;
    public int c_id; //PRIMARY KEY 1
    public int c_d_id; //PRIMARY KEY 2
    public int c_w_id; //PRIMARY KEY 3
    public String c_first;
    public String c_middle;
    public String c_last;
    public String c_street_1;
    public String c_street_2;
    public String c_city;
    public String c_state;
    public int c_zip;
    public String c_phone;
    public long c_since;
    public String c_credit;
    public int c_credit_lim;
    public double c_discount;
    public double c_balance;
    public double c_ytd_payment;
    public int c_payment_cnt;
    public int c_delivery_cnt;
    public String c_data;

    public Customer() {
    }

    public Customer(ObjId id) {
        this.setId(id);
    }

    public Customer(int c_id, int c_d_id, int c_w_id, String c_last) {
        this.c_id = c_id;
        this.c_w_id = c_w_id;
        this.c_d_id = c_d_id;
        this.c_last = c_last;
        this.setStrObjId();
    }

    @Override
    public Message getSuperDiff() {
        return new Message(this.c_d_id, this.c_w_id, this.c_first, this.c_middle, this.c_last, this.c_street_1, this.c_street_2, this.c_city, this.c_state, this.c_zip, this.c_phone, this.c_since, this.c_credit, this.c_credit_lim, this.c_discount, this.c_balance, this.c_ytd_payment, this.c_payment_cnt, this.c_delivery_cnt, this.c_data);
    }

    @Override
    public void updateFromDiff(Message objectDiff) {
        this.c_d_id = (int) objectDiff.getNext();
        this.c_w_id = (int) objectDiff.getNext();
        this.c_first = (String) objectDiff.getNext();
        this.c_middle = (String) objectDiff.getNext();
        this.c_last = (String) objectDiff.getNext();
        this.c_street_1 = (String) objectDiff.getNext();
        this.c_street_2 = (String) objectDiff.getNext();
        this.c_city = (String) objectDiff.getNext();
        this.c_state = (String) objectDiff.getNext();
        this.c_zip = (int) objectDiff.getNext();
        this.c_phone = (String) objectDiff.getNext();
        this.c_since = (long) objectDiff.getNext();
        this.c_credit = (String) objectDiff.getNext();
        this.c_credit_lim = (int) objectDiff.getNext();
        this.c_discount = (double) objectDiff.getNext();
        this.c_balance = (double) objectDiff.getNext();
        this.c_ytd_payment = (double) objectDiff.getNext();
        this.c_payment_cnt = (int) objectDiff.getNext();
        this.c_delivery_cnt = (int) objectDiff.getNext();
        this.c_data = (String) objectDiff.getNext();
    }

    @Override
    public String toString() {
        return (
                "\n***************** Customer ********************" +
                        "\n*           c_id = " + c_id +
                        "\n*         c_d_id = " + c_d_id +
                        "\n*         c_w_id = " + c_w_id +
                        "\n*     c_discount = " + c_discount +
                        "\n*       c_credit = " + c_credit +
                        "\n*         c_last = " + c_last +
                        "\n*        c_first = " + c_first +
                        "\n*   c_credit_lim = " + c_credit_lim +
                        "\n*      c_balance = " + c_balance +
                        "\n*  c_ytd_payment = " + c_ytd_payment +
                        "\n*  c_payment_cnt = " + c_payment_cnt +
                        "\n* c_delivery_cnt = " + c_delivery_cnt +
                        "\n*     c_street_1 = " + c_street_1 +
                        "\n*     c_street_2 = " + c_street_2 +
                        "\n*         c_city = " + c_city +
                        "\n*        c_state = " + c_state +
                        "\n*          c_zip = " + c_zip +
                        "\n*        c_phone = " + c_phone +
                        "\n*        c_since = " + c_since +
                        "\n*       c_middle = " + c_middle +
                        "\n*         c_data = " + c_data +
                        "\n**********************************************"
        );
    }

    @Override
    public String getObjIdString() {
        return model.getName() + ":c_w_id=" + c_w_id + ":c_d_id=" + c_d_id + ":c_id=" + c_id;
    }

    @Override
    public String[] getPrimaryKeys() {
        return new String[]{"c_w_id", "c_d_id", "c_id"};
    }

    @Override
    public String getModelName() {
        return this.model.getName();
    }

    @Override
    public String getCSVHeader() {
        return "c_w_id,c_d_id,c_id,c_last,c_first,c_state,c_zip,c_discount,c_middle,c_ytd_payment,c_balance,c_phone,c_since,c_credit_lim,c_delivery_cnt,c_payment_cnt,model,c_street_1,c_city,c_street_2,c_credit,c_data";
    }
}
