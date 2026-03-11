package com.pointblue.idm.eventlogger.offline;
//  jndiSocketFactory.java
//
//  Created by Jerry Combs on 7/7/05.
//

//import com.sun.net.ssl.*;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class JndiSocketFactory extends SSLSocketFactory implements Runnable
{

    private SSLSocketFactory factory = null;
    private JndiSocketFactory default_factory = null;
    private ClassLoader myClassLoader = null;
    private static final int TIMEOUT = 5000;
    private static final long TIME = 5000;
    private SSLContext sslctx;



    protected String host;
    protected int port;
    protected Socket socket = null;
    protected IOException socketError = null;
    protected boolean hasTimedOut = false;

    public  void setClassLoader(ClassLoader newLoader)
    {
        myClassLoader = newLoader;
    }

    private  ClassLoader getClassLoader()
    {
        if (myClassLoader == null)
        {
            myClassLoader = ClassLoader.getSystemClassLoader();
        }
        return myClassLoader;
    }

    public  void setDebugOn()
    {
        System.setProperty("javax.net.debug", "ssl handshake verbose");
    }

    public JndiSocketFactory()
    {

    }

    public  static SocketFactory getDefault()
    {
        synchronized (JndiSocketFactory.class)
        {

            return new JndiSocketFactory();

        }
    }

    public Socket createSocket(String hostName, int port2)
            throws IOException, UnknownHostException
    {
        //System.out.println("create s1 called");
        try
        {
            sslctx = SSLContext.getInstance("TLS");
            TrustManager[] myTrustMgr = new TrustManager[]
                    {
                            new DummyTrustManager()
                    };
            sslctx.init(null, myTrustMgr, null);

        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
        factory = sslctx.getSocketFactory();


        this.host = hostName;
        this.port = port2;
        this.socket = null;
        this.socketError = null;
        hasTimedOut = false;

        Thread r = new Thread(this);
        r.setDaemon(true);
        r.start();
        try
        {
            r.join(TIME);

        }
        catch (InterruptedException ie)
        {
            r.interrupt();
        }
        if (socketError != null)
        {
            throw socketError;
        }
        if (socket == null)
        {
            hasTimedOut = true;
            throw new IOException("Socket connection timed out: " + host + ":" + port);
        }
        else
        {
            //System.out.println("Connected");
        }

        return socket;

    }

    public Socket createSocket(InetAddress host, int port)
            throws IOException, UnknownHostException
    {
        return factory.createSocket(host, port);
    }

    public Socket createSocket(InetAddress host, int port, InetAddress client_host, int client_port)
            throws IOException, UnknownHostException
    {
        return factory.createSocket(host, port, client_host, client_port);
    }

    public Socket createSocket(String host, int port, InetAddress client_host, int client_port)
            throws IOException, UnknownHostException
    {
        return factory.createSocket(host, port, client_host, client_port);
    }

    public Socket createSocket(Socket socket, String host, int port, boolean autoclose)
            throws IOException, UnknownHostException
    {
        return factory.createSocket(socket, host, port, autoclose);
    }

    public String[] getDefaultCipherSuites()
    {
        return factory.getDefaultCipherSuites();
    }

    public String[] getSupportedCipherSuites()
    {
        return factory.getSupportedCipherSuites();
    }

    public void run()
    {
        try
        {
            socket = factory.createSocket(host, port);
        }
        catch (IOException ioe)
        {
            socketError = ioe;
        }
        if (hasTimedOut)
        {
            try
            {
                this.socket.close();
            }
            catch (IOException ioe)
            {
                //eat it
            };
            socket = null;
            socketError = null;
        }
    }

}
