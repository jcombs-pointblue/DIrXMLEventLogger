package com.pointblue.idm.eventlogger.offline;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;


public class LoadTestGenerator {

    /**
     * The main method to execute the comparison.
     *
     * @param args Command line arguments. Expects 6 arguments:
     *             <input-file> <attribute-to-extract> <ldap-url> <base-dn> <ldap-username> <ldap-password>
     */
    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: java com.pointblue.idm.eventlogger.offline.LoadtestGenerator  <ldap-url> <base-dn> <ldap-username> <ldap-password>");
            System.exit(1);
        }


        String ldapUrl = args[0];
        String baseDN = args[1];
        String ldapUsername = args[2];
        String ldapPassword = args[3];

        try{
            Hashtable<String, String> env = new Hashtable<>();
            if(ldapUrl.startsWith("ldaps://")) {
                env.put(Context.SECURITY_PROTOCOL, "ssl");
                env.put("java.naming.ldap.factory.socket",
                        "com.pointblue.idm.eventlogger.offline.JndiSocketFactory");
            }
            env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
            env.put(Context.PROVIDER_URL, ldapUrl);
            env.put(Context.SECURITY_AUTHENTICATION, "simple");
            env.put(Context.SECURITY_PRINCIPAL, ldapUsername);
            env.put(Context.SECURITY_CREDENTIALS, ldapPassword);

            DirContext ctx = new InitialDirContext(env);

            System.out.println("Connected to LDAP server at " + ldapUrl);
            for(int i=0; i< 10000; i++) {
                String dn = "cn=TestUserZZ" + i + "," + baseDN;
                Attributes attrs = new BasicAttributes();
                attrs.put("objectClass", "inetOrgPerson");
                attrs.put("cn", "TestUser" + i);
                attrs.put("sn", "User" + i);
                attrs.put("mail", "testuser" + i + "@example.com");
                attrs.put("carLicense", "jh;jhlkjhlkjh;lkjljhljhlkjhlkjsdfas;dkjfhsdolsiDJCV;lskdjvc;slKDJV;ASLKDJV;ASLKDJV;LASKJDV;LKJ;ASLDKJVLKAJSDVLKJASD;VLKJ;ASLDKJV;ALKSDJV;LAKSDV;LKASDJ;VLKJ");

                ctx.createSubcontext(dn, attrs);
                System.out.println("Created entry: " + dn);
            }

        }catch (Exception e) {
            System.err.println("An error occurred: " + e.getMessage());
            e.printStackTrace();
        }



    }


}