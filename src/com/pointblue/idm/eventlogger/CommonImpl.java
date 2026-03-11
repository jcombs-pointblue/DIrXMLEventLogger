/**
 * **********************************************************************
 * Copyright � 1999-2002 Novell, Inc. All Rights Reserved.
 * <p>
 * THIS WORK IS SUBJECT TO U.S. AND INTERNATIONAL COPYRIGHT LAWS AND
 * TREATIES. USE AND REDISTRIBUTION OF THIS WORK IS SUBJECT TO THE LICENSE
 * AGREEMENT ACCOMPANYING THE SOFTWARE DEVELOPMENT KIT (SDK) THAT CONTAINS
 * THIS WORK. PURSUANT TO THE SDK LICENSE AGREEMENT, NOVELL HEREBY GRANTS
 * TO DEVELOPER A ROYALTY-FREE, NON-EXCLUSIVE LICENSE TO INCLUDE NOVELL'S
 * SAMPLE CODE IN ITS PRODUCT. NOVELL GRANTS DEVELOPER WORLDWIDE DISTRIBUTION
 * RIGHTS TO MARKET, DISTRIBUTE, OR SELL NOVELL'S SAMPLE CODE AS A COMPONENT
 * OF DEVELOPER'S PRODUCTS. NOVELL SHALL HAVE NO OBLIGATIONS TO DEVELOPER OR
 * DEVELOPER'S CUSTOMERS WITH RESPECT TO THIS CODE.
 * <p>
 * *************************************************************************
 */
package com.pointblue.idm.eventlogger;

import com.novell.nds.dirxml.driver.Trace;
import com.novell.nds.dirxml.driver.XmlDocument;
import com.novell.xsl.util.Util;
import org.w3c.dom.*;
import org.w3c.dom.CharacterData;

import java.util.HashMap;
import java.util.Map;

/**
 * Common implementation code for DirXML skeleton driver.
 * <p>
 * This class contains a number of utility methods useful for creating and
 * traversing XDS documents, and contains some common state data.
 *
 * @version 2.0 28Jun2000
 */
public class CommonImpl {

    /**
     * Parameter to status document creation methods for level attribute of
     * "success".
     */
    public static final int STATUS_SUCCESS = 0;
    /**
     * Parameter to status document creation methods for level attribute of
     * "fatal".
     */
    public static final int STATUS_FATAL = 1;
    /**
     * Parameter to status document creation methods for level attribute of
     * "error".
     */
    public static final int STATUS_ERROR = 2;
    /**
     * Parameter to status document creation methods for level attribute of
     * "warning".
     */
    public static final int STATUS_WARNING = 3;
    /**
     * Parameter to status document creation methods for level attribute of
     * "retry".
     */
    public static final int STATUS_RETRY = 4;
    /**
     * Array used to translate int level parameters to XML attribute values
     */
    protected static final String[] STATUS_LEVELS
            = {
            "success",
            "fatal",
            "error",
            "warning",
            "retry"
    };
    /**
     * Instance of com.novell.nds.dirxml.driver.Trace, for use by derived class
     * to output trace messages to DSTrace screen and DirXML-JavaTraceFile
     * specified file.
     * <p>
     * To cause trace messages to appear on the DSTrace screen set the
     * DirXML-DriverTraceLevel attribute on the driver set object to a value
     * greater than zero. To log trace messages set the DirXML-JavaTraceFile
     * attribute on the driver set object to a filename value.
     */
    protected Trace tracer;

    String driverRDN;


    /**
     * Construct an instance of CommonImpl, using the passed string as the
     * message prologue for TRACE messages.
     *
     * @param traceHeader Message prologue for TRACE messages
     */
    protected CommonImpl(String traceHeader) {
        tracer = new Trace(traceHeader);
    }

    public static String getText(Node node) {
        StringBuilder reply = new StringBuilder();

        NodeList children = node.getChildNodes();
        for (int i = 0; i < children.getLength(); i++)
        {
            Node child = children.item(i);

            if ((child instanceof CharacterData && !(child instanceof Comment)) || child instanceof EntityReference)
            {
                reply.append(child.getNodeValue());
            } else
            {
                if (child.getNodeType() == Node.ELEMENT_NODE)
                {
                    reply.append(getText(child));
                }
            }
        }

        return reply.toString();
    }

    private static String getRDN(String dn) {
        String rdn = dn;
        int index = rdn.lastIndexOf("\\");
        if (index != -1 && index < rdn.length())
            rdn = rdn.substring(index + 1);
        return rdn;
    }

    /**
     * Create a return status document for DirXML that signals success
     *
     * @return An XmlDocument with a status element, level="success"
     */
    protected XmlDocument createSuccessDocument() {
        return createStatusDocument(STATUS_SUCCESS, null);
    }

    /**
     * Create an XDS status document for returning status to DirXML.
     *
     * @param level STATUS_SUCCESS, STATUS_FATAL, STATUS_ERROR, STATUS_WARNING,
     * or STATUS_RETRY
     * @param message A detail message that will be the content of the status
     * element (may be null)
     */
    protected XmlDocument createStatusDocument(int level, String message) {
        //create a document for returning something to DirXML
        Element output = createOutputDocument();
        //add the status element
        addStatusElement(output, level, message, null);
        //return an XmlDocument object suitable for returning to DirXML
        return new XmlDocument(output.getOwnerDocument());
    }

    /**
     * Add a &lt;status> element to an XDS input or output document. The
     * document must already have an &lt;output> element or an &lt;input>
     * element.
     *
     * @param parent The input or output element in the XDS document.
     * @param level STATUS_SUCCESS, STATUS_FATAL, STATUS_ERROR, STATUS_WARNING,
     * or STATUS_RETRY
     * @param message A detail message that will be the content of the status
     * element (may be null)
     * @param eventId The event id to which the status element corresponds (may
     * be null)
     * @return The added status element.
     */
    protected Element addStatusElement(Element parent, int level, String message, String eventId) {
        //get the DOM Document for use as a factory
        Document document = parent.getOwnerDocument();
        //create the status element and place it under the parent element
        Element status = document.createElementNS(null, "status");
        parent.appendChild(status);
        //set the level based on what was passed
        status.setAttributeNS(null, "level", STATUS_LEVELS[level]);
        //if we have an event id, set the attribute
        if (eventId != null && eventId.length() > 0)
        {
            status.setAttributeNS(null, "event-id", eventId);
        }
        //if there is a detail message, put in in as the content of the status element
        if (message != null && message.length() > 0)
        {
            Text msg = document.createTextNode(message);
            status.appendChild(msg);
        }
        return status;
    }

    /**
     * Create a bare-bones XDS document for DirXML use, containing only the root
     * &lt;nds> element.
     *
     * @return The root nds Element
     */
    protected Element createXdsDocument() {
        //create a DOM Document using the document factory
        Document returnDoc = com.novell.xml.dom.DocumentFactory.newDocument();
        //create the <nds> root element
        Element nds = returnDoc.createElementNS(null, "nds");
        returnDoc.appendChild(nds);
        //set the various xds attributes
        nds.setAttributeNS(null, "dtdversion", "4.0");
        return nds;
    }

    /**
     * Return the association value for a command element (add, modify, etc.)
     *
     * @param command The command element
     * @return The association value, or null if no association found.
     */
    protected String getAssociation(Element command) {
        //find the association element as a child of the command
        Node childNode = command.getFirstChild();
        while (childNode != null)
        {
            if (childNode.getNodeType() == Node.ELEMENT_NODE
                    && childNode.getNodeName().equals("association"))
            {
                return com.novell.xsl.util.Util.getXSLStringValue(childNode);
            }
            childNode = childNode.getNextSibling();
        }
        return null;
    }

    /**
     * Create an empty "output" document for returning something to DirXML.
     * Return the output element.
     *
     * @return the output <code>Element</code>.
     */
    protected Element createOutputDocument() {
        //create the basic XDS document
        Element nds = createXdsDocument();
        //add an output element
        Element output = nds.getOwnerDocument().createElementNS(null, "output");
        nds.appendChild(output);
        return output;
    }

    /**
     * Create an empty "input" document for submitting something (publishing or
     * subscriber query) to DirXML. Return the input element.
     *
     * @return the input <code>Element</code>.
     */
    protected Element createInputDocument() {
        //create the basic XDS document
        Element nds = createXdsDocument();
        //add an input element
        Element input = nds.getOwnerDocument().createElementNS(null, "input");
        nds.appendChild(input);
        return input;
    }

    public String getEventID(Element eventElement) {
        return eventElement.getAttribute("event-id");
    }

    /**
     * Add a driver-state, subscriber-state, or publisher-state element to an
     * input or an output document and then add a shim-specific element with the
     * specified value.
     * <p>
     * The shims can write state information into NDS using either return
     * documents, or in the case of the publisher, submitted documents. This
     * state information is passed to the shim as part of the init document
     * passed to the shim init() method.
     * <p>
     * This will add the init-state and driver-state, subscriber-state, or
     * publisher-state elements if necessary, and will then append an element
     * with the passed name and content.
     *
     * @param parent The input or output element in the XDS document.
     * @param stateName "driver-state", "subscriber-state", or "publisher-state"
     * @param elementName The name of the shim-specific state element.
     * @param elementValue The value to place as content of the shim-specific
     * state element.
     * @return The created element.
     */
    protected Element addState(Element parent, String stateName, String elementName, String elementValue) {
        Document document = parent.getOwnerDocument();
        Element stateElement;
        //see if we already have an init-params element
        Element initParams = (Element) parent.getElementsByTagNameNS(null, "init-params").item(0);
        if (initParams == null)
        {
            initParams = document.createElementNS(null, "init-params");
            parent.appendChild(initParams);
            stateElement = document.createElementNS(null, stateName);
            initParams.appendChild(stateElement);
        } else
        {
            //have init-params, see if we have the requisite state element
            stateElement = (Element) initParams.getElementsByTagNameNS(null, stateName).item(0);
            if (stateElement == null)
            {
                stateElement = document.createElementNS(null, stateName);
                initParams.appendChild(stateElement);
            }
        }
        //add the passed element and content
        Element element = document.createElementNS(null, elementName);
        stateElement.appendChild(element);
        //create the content, if it was passed
        if (elementValue != null && elementValue.length() > 0)
        {
            Text value = document.createTextNode(elementValue);
            element.appendChild(value);
        }
        return element;
    }

    /**
     * Extract and return the authentication parameters from an initialization
     * document.
     *
     * @param initDocument The XML document passed to DriverShim.init(),
     * SubscriberShim.init(), or PublicationShim.init().
     *
     * @return An instance of AuthenticationParams containing any authentication
     * parameters found in the init document. Any or all of the strings may be
     * null if no authentication parameters were entered for the driver in
     * ConsoleOne. Typically, a driver would return an error if a required
     * parameter is missing.
     */
    protected AuthenticationParams getAuthenticationParams(Document initDocument) {
        AuthenticationParams params = new AuthenticationParams();
        //get the <init-params> element
        Element initParams = (Element) initDocument.getElementsByTagNameNS(null, "init-params").item(0);
        if (initParams == null)
        {
            //no <init-params> element found (shouldn't happen)
            return params;
        }
        //find <authentication-info> element
        Element authInfo = (Element) initDocument.getElementsByTagNameNS(null, "authentication-info").item(0);
        if (authInfo == null)
        {
            //no <authentication-info> element (may happen if nothing entered in ConsoleOne)
            return params;
        }
        //look for <server> - this corresponds to the "Authentication context" field
        Element server = (Element) authInfo.getElementsByTagNameNS(null, "server").item(0);
        if (server != null)
        {
            //get the string value of the <server> element using a handy utility function
            //found in Novell's XSLT implemenation (which will always be available for DirXML
            //drivers)
            params.authenticationContext = com.novell.xsl.util.Util.getXSLStringValue(server);
        }
        //look for <user> - this corresponds to the "Authentication ID" field
        Element user = (Element) authInfo.getElementsByTagNameNS(null, "user").item(0);
        if (user != null)
        {
            //get string value of <user> element
            params.authenticationId = com.novell.xsl.util.Util.getXSLStringValue(user);
        }
        //look for <password> - this corresponds to the "Application Password" field
        Element password = (Element) authInfo.getElementsByTagNameNS(null, "password").item(0);
        if (password != null)
        {
            //get string value of <password> element
            params.applicationPassword = com.novell.xsl.util.Util.getXSLStringValue(password);
        }
        return params;
    }

    /**
     * Extract shim options parameters from the &lt;driver-options>,
     * &lt;subscriber-options>, or &lt;publisher-options> element in an
     * initialization document passed to <code>DriverShim.init()</code>,
     * <code>SubscriptionShim.init()</code>, or
     * <code>PublicationShim.init()</code>. This will also find information in
     * the &lt;driver-state>, &lt;subscriber-state>, or &lt;publisher-state>
     * element.
     * <p>
     * Since this is a general, illustrative method, the options are described
     * by an array of <code>ShimParamDesc</code> objects.
     * <p>
     * Note that all elements under the -options element and corresponding
     * -state element must have unique names.
     *
     * @param initDocument The init document passed to the shim
     * <code>init()</code> method.
     * @param shimName "driver", "subscriber", or "publisher". This is used to
     * find "driver-options", "subscriber-options", or "publisher-options", and
     * to find "driver-state", "subscriber-state", or "publisher-state"
     * @param paramDesc An array of ShimParamDesc objects that describes the
     * options to find by name and type (String or int).
     * @return A ShimParams object containing the options values found.
     * @exception IllegalArgumentException is thrown if a parameter
     * that is flagged as required is not found or has no value.
     */
    protected ShimParams getShimParams(Document initDocument, String shimName, ShimParamDesc[] paramDesc)
            throws IllegalArgumentException {
        int i;
        ShimParams params = new ShimParams();
        //find the options element desired
        String optionsName = shimName + "-options";
        tracer.trace("Looking for options element: " + optionsName, 3);
        tracer.trace(initDocument);
        Element optionsElement = (Element) initDocument.getElementsByTagNameNS(null, optionsName).item(0);

        //Element optionsElement = initDocument.getElementById(optionsName);

        tracer.trace("optionsElement: " + optionsElement);
        //get any interesting values
        extractValues(optionsElement, params, paramDesc);
        //find the state element desired
        String stateName = shimName + "-state";
        Element stateElement = (Element) initDocument.getElementsByTagNameNS(null, stateName).item(0);
        //get any interesting values
        extractValues(stateElement, params, paramDesc);
        StringBuffer errorList = new StringBuffer(128);
        int errorCount = 0;
        //check for any required options that weren't found
        for (i = 0; i < paramDesc.length; ++i)
        {
            if (paramDesc[i].required && !params.haveParam(paramDesc[i].paramName))
            {
                ++errorCount;
                errorList.append('\n');
                errorList.append("missing required option: '").append(paramDesc[i].paramName).append("'");
            }
        }
        if (errorCount > 0)
        {
            throw new IllegalArgumentException(errorList.toString());
        }
        return params;
    }

    /**
     * Extract values from a driver-options, subscriber-options,
     * publisher-options, driver-state, subscriber-state, or publisher-state
     * element.
     *
     * @param optionsElement The element from which to extract values.
     * @param params The ShimParams object to which to add the values.
     * @param paramDesc An array of ShimParamDesc objects that describes the
     * options to find by name and type (String or int).
     */
    private void extractValues(Element optionsElement, ShimParams params, ShimParamDesc[] paramDesc) {
        if (optionsElement != null)
        {
            int i;
            //iterate through the param descriptions, looking for the described values
            Element option;
            for (i = 0; i < paramDesc.length; ++i)
            {
                option = (Element) optionsElement.getElementsByTagNameNS(null, paramDesc[i].paramName).item(0);
                if (option != null)
                {
                    //found the element, get the content and interpret it
                    String content = com.novell.xsl.util.Util.getXSLStringValue(option);
                    if (content == null || content.length() == 0)
                    {
                        //empty content doesn't count
                        continue;
                    }
                    if (paramDesc[i].paramType == ShimParamDesc.STRING_TYPE)
                    {
                        //string type
                        params.putStringParam(paramDesc[i].paramName, content);
                        tracer.trace("Param: " + paramDesc[i].paramName + " :" + content);
                    } else
                    {
                        //int type
                        try
                        {
                            int value = Integer.parseInt(content);
                            params.putIntParam(paramDesc[i].paramName, value);
                        } catch (NumberFormatException e)
                        {
                            throw new IllegalArgumentException("option '" + paramDesc[i].paramName + "' must be integer type");
                        }
                    }
                }
            }    //for
        } else
        {
            tracer.trace("options element was null");
        }
    }

    void setDriverRDN(Document doc) {
        if (this.driverRDN.length() == 0)
        {
            Element initParams = (Element) doc.getElementsByTagNameNS(null, "init-params").item(0);
            if (initParams != null)
            {
                String dn = initParams.getAttributeNS(null, "src-dn");
                this.driverRDN = getRDN(dn);
            }
        }
    }

    int getHeartbeatInterval(Document initDoc) {
        long interval;
        Element optionsElement = (Element) initDoc.getElementsByTagNameNS(null, "publisher-options").item(0);
        if (optionsElement == null)
            return 0;
        Element heartbeatElement = (Element) optionsElement.getElementsByTagNameNS(null, "pub-heartbeat-interval").item(0);
        if (heartbeatElement == null)
            return 0;
        String value = Util.getXSLStringValue(heartbeatElement);
        try
        {
            interval = Long.parseLong(value);
        } catch (NumberFormatException e)
        {
            return -1;
        }
        interval *= 60000L;
        if (interval > 2147483647L)
            interval = -1L;
        return (int) interval;
    }


    /**
     * Class for use in storing authentication &lt;init-params> values from the
     * initialization documents passed to DriverShim.init(),
     * SubscriptionShim.init(), or PublicationShim.init()
     * <p>
     * These Strings correspond to fields found in ConsoleOne on the
     * DirXML-Driver object properties dialog, under DirXML, Driver
     * Configuration, Authentication.
     * <p>
     * The fields are intended to be used as follows (but a driver can, of
     * course, use them in any way appropriate to the driver and its supported
     * application):
     * <p>
     * <b>Authentication ID</b>: Stores the User ID or other ID used for the
     * driver to authenticate or connect to the application.
     * <p>
     * <b>Authentication context</b>: Stores a server name, IP address, or other
     * information used to inform the driver to which application instance or
     * server the driver is to connect.
     * <p>
     * <b>Application password</b>: Stores in a secure fashion the application
     * password the driver needs to authenticate to the application.
     */
    static class AuthenticationParams {

        /**
         * Corresponds to the "Authentication ID" field
         */
        public String authenticationId = null;
        /**
         * Corresponds to the "Authentication context" field
         */
        public String authenticationContext = null;
        /**
         * Corresponds to the "Application Password" field
         */
        public String applicationPassword = null;
    }

    /**
     * Class used to build an array of options element descriptions for use in
     * getShimOptions().
     * <p>
     * Each options element is described in terms of element name and how the
     * content should be interpreted.
     */
    static class ShimParamDesc {

        /**
         * Options element content type is a string.
         */
        public static final int STRING_TYPE = 0;
        /**
         * Options element content type is an int.
         */
        public static final int INT_TYPE = 1;
        public String paramName;
        public int paramType;
        public boolean required;

        /**
         * Construct.
         *
         * @param name XML name of options element.
         * @param type STRING_TYPE or INT_TYPE.
         */
        public ShimParamDesc(String name, int type, boolean required) {
            paramName = name;
            paramType = type;
            this.required = required;
        }
    }

    /**
     * Class to store values from a &lt;driver-options>,
     * &lt;subscriber-options>, or &lt;publisher-options> element from an init
     * document.
     * <p>
     * Values are keyed by the element name in the options XML, and may be
     * either Strings or ints. This is not the most efficient way to store such
     * things, but it needs to be general to serve as an example for the
     * skeleton driver.
     */
    public static class ShimParams {

        //parameter values keyed by element name
        private final Map<String, Object> paramMap = new HashMap<>();

        /**
         * Set a string parameter value by name.
         *
         * @param name The name of the parameter (options element name)
         * @param value The value of the parameter.
         */
        public void putStringParam(String name, String value) {
            paramMap.put(name, value);
        }

        /**
         * Get a string parameter value by name.
         *
         * @param name The name of the parameter (options element name)
         * @return The value of the parameter.
         */
        public String getStringParam(String name) {
            return (String) paramMap.get(name);
        }

        /**
         * Set an integer parameter value by name.
         *
         * @param name The name of the parameter (options element name)
         * @param value The value of the parameter.
         */
        public void putIntParam(String name, int value) {
            paramMap.put(name, Integer.valueOf(value));
        }

        /**
         * Get an integer parameter value by name.
         *
         * @param name The name of the parameter (options element name)
         * @return The value of the parameter.
         */
        public int getIntParam(String name) {
            int retVal = -1;
            Integer intObject = (Integer) paramMap.get(name);
            if (intObject != null)
            {
                retVal = intObject.intValue();
            }
            return retVal;
        }

        /**
         * Return <code>true</code> if a param with the passed name is in the
         * collection.
         *
         * @param name The name of the parameter (options element name)
         * @return <code>true</code> if the param exists in the collection.
         */
        public boolean haveParam(String name) {
            return paramMap.containsKey(name);
        }
    }
}
