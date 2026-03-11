package com.pointblue.idm.eventlogger.offline.SubShimTest;

import com.novell.nds.dirxml.driver.DriverShim;
import com.novell.nds.dirxml.driver.SubscriptionShim;
import com.novell.nds.dirxml.driver.XmlDocument;
import com.pointblue.idm.eventlogger.offline.TestJig;
import com.pointblue.idm.eventlogger.offline.TestQueryProcessor;

public class Adduser1 extends TestJig {
    public static void main(String[] args) {

        //Trace.registerImpl(TraceImpl.class, 100);
        new Adduser1().run(initPath+"initParams.xml", eventPath+"Adduser1.xml");
    }

    @Override
    public void run(String initDocPath, String xdsDocPath) {
        // Trace.registerImpl(TraceImpl.class, 100);
        // Trace tracer = new Trace("test");
        DriverShim theDriver = getDriver();
        theDriver.init(getInitXML(initDocPath));
        System.out.println("Driver initialized");

        SubscriptionShim subscriber = theDriver.getSubscriptionShim();
        subscriber.init(getInitXML(initDocPath));
        System.out.println("Subscriber initialized");

        XmlDocument xdsDoc = getXdsDoc(xdsDocPath);

        XmlDocument outputDoc = subscriber.execute(xdsDoc, new TestQueryProcessor());
        System.out.println(outputDoc.getDocumentString());
        //  tracer.trace("outputDoc: " + outputDoc);

    }
}
