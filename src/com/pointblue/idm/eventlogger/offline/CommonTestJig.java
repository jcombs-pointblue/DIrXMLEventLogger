package com.pointblue.idm.eventlogger.offline;

import com.novell.nds.dirxml.driver.DriverShim;
import com.novell.nds.dirxml.driver.XmlDocument;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class CommonTestJig {
    public static DriverShim getDriver() {
        return new com.pointblue.idm.eventlogger.EventLoggerDriver();
    }

    public static XmlDocument getInitXML(String initDocPath) {
        InputStream istream = TestJig.class.getResourceAsStream(initDocPath);
        String initString = CommonTestJig.getStringFromInputStream(istream);
        //System.out.println(initString);
        return new XmlDocument(initString);
    }

    public static XmlDocument getXdsDoc(String xdsDocPath) {
        InputStream istream = TestJig.class.getResourceAsStream(xdsDocPath);

        String initString = getStringFromInputStream(istream);
        //System.out.println(initString);
        return new XmlDocument(initString);
    }

    public static String getStringFromInputStream(InputStream is) {

        BufferedReader br = null;
        StringBuilder sb = new StringBuilder();

        String line;
        try
        {

            br = new BufferedReader(new InputStreamReader(is));
            while ((line = br.readLine()) != null)
            {
                sb.append(line);
            }

        } catch (IOException e)
        {
            e.printStackTrace();
        } finally
        {
            if (br != null)
            {
                try
                {
                    br.close();
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }

        return sb.toString();

    }
}
