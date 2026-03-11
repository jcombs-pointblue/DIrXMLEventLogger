/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pointblue.idm.eventlogger.offline;

import com.novell.nds.dirxml.driver.TraceInterface;
import com.novell.nds.dirxml.driver.XmlDocument;

/**
 * @author jcombs
 */
public class TraceImpl implements TraceInterface {
    public void trace(int i, String string) {
        System.out.println(string);
    }

    public void trace(int i, int i1, String string) {
        System.out.println(string);

    }

    public void trace(int i, XmlDocument xd) {
        System.out.println(xd);

    }

    public int getLevel() {
        return 3;
    }

}
