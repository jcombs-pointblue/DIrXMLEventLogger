//
//  DummyTrustManager.java
//  This class is used so that LDAPS connections can be made without verifying the cert
//
//  Created by Jerry Combs on 7/11/05.
//  Copyright 2005 __MyCompanyName__. All rights reserved.
//
package com.pointblue.idm.eventlogger.offline;

import javax.net.ssl.X509TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;


public class DummyTrustManager implements X509TrustManager
{

    // SSLContext ctx = SSLContext.getInstance("TLS");
    // ctx.init(null, myTM, null);

    //ssl.TrustManagerFactory.algorithm=SunX509

    //Security.setProperty("ssl.TrustManagerFactory.algorithm",
    //                     "dummyTrust");

    public void checkClientTrusted(X509Certificate[] chain, String authType)
            throws CertificateException
    {
        return;
    }

    public void checkServerTrusted(X509Certificate[] chain, String authType)
            throws CertificateException
    {
        return;
    }

    public X509Certificate[] getAcceptedIssuers()
    {
        //throw new RuntimeException("NOT IMPLEMENTED");
        return new X509Certificate[0];
    }
}
