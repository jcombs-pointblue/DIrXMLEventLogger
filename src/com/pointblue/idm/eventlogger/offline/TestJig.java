package com.pointblue.idm.eventlogger.offline;

import com.novell.nds.dirxml.driver.Trace;

//import org.w3c.dom.

/**
 * @author jcombs
 */
public abstract class TestJig extends CommonTestJig {

public static String eventPath = "/com/pointblue/idm/eventlogger/offline/events/";
public static String initPath = "/com/pointblue/idm/eventlogger/offline/";

    /*
      1. Instantiate drivershim
      2. Call init method
      3. get subscrirber
      4. Submit doc


     */

    public TestJig() {
        Trace.registerImpl(TraceImpl.class, 100);
    }


    public abstract void run(String initDocPath, String xdsDocPath);


}
