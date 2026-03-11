/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pointblue.idm.eventlogger.offline;

import com.novell.nds.dirxml.driver.XmlDocument;
import com.novell.nds.dirxml.driver.XmlQueryProcessor;

/**
 * @author jcombs
 */
public class TestQueryProcessor implements XmlQueryProcessor {
    public XmlDocument query(XmlDocument xd) {
        //For now just return the sent document
        System.out.println("TestQueryProcessor:query");
        return xd;
    }

}
