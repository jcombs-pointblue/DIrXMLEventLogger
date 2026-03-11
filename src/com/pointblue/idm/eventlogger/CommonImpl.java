package com.pointblue.idm.eventlogger;

import com.novell.nds.dirxml.driver.Trace;
import com.novell.nds.dirxml.driver.XmlDocument;
import com.novell.xsl.util.Util;
import org.w3c.dom.*;
import org.w3c.dom.CharacterData;

import java.util.HashMap;
import java.util.Map;

/**
 * Common implementation code shared by DirXML driver shims.
 * <p>
 * Provides utility methods for creating and traversing XDS documents, managing
 * status responses, extracting initialization parameters, and maintaining common
 * driver state. This class serves as the base for {@link EventLoggerDriver} and
 * any future shim implementations.
 * <p>
 * Originally derived from the Novell DirXML skeleton driver SDK.
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
     * Array used to translate int level parameters to XML attribute values.
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
     * Trace instance for outputting messages to DSTrace and DirXML-JavaTraceFile.
     * <p>
     * To cause trace messages to appear on the DSTrace screen, set the
     * DirXML-DriverTraceLevel attribute on the driver set object to a value
     * greater than zero. To log trace messages to a file, set the
     * DirXML-JavaTraceFile attribute on the driver set object.
     */
    protected Trace tracer;

    /**
     * The relative distinguished name of the driver object in eDirectory.
     * Set during initialization from the {@code src-dn} attribute of the
     * {@code init-params} element.
     */
    String driverRDN;



    /**
     * Constructs a CommonImpl instance with the given trace message prologue.
     *
     * @param traceHeader message prologue prepended to all trace output
     */
    protected CommonImpl(String traceHeader) {
        tracer = new Trace(traceHeader);
    }

    /**
     * Recursively extracts the text content from a DOM node and all its descendants.
     * Concatenates text from {@link CharacterData} nodes (excluding comments) and
     * {@link EntityReference} nodes.
     *
     * @param node the DOM node to extract text from
     * @return the concatenated text content of the node and its children
     */
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

    /**
     * Extracts the relative distinguished name (leaf component) from a full
     * backslash-delimited eDirectory DN.
     *
     * @param dn the full distinguished name (e.g. {@code \TREE\org\Users\jdoe})
     * @return the RDN (e.g. {@code jdoe}), or the original string if no backslash is found
     */
    private static String getRDN(String dn) {
        String rdn = dn;
        int index = rdn.lastIndexOf("\\");
        if (index != -1 && index < rdn.length())
            rdn = rdn.substring(index + 1);
        return rdn;
    }


    /**
     * Creates a return status document for DirXML that signals success.
     *
     * @return an XmlDocument with a status element at level "success"
     */
    protected XmlDocument createSuccessDocument() {
        return createStatusDocument(STATUS_SUCCESS, null);
    }


    /**
     * Creates an XDS status document for returning status to DirXML.
     *
     * @param level   one of {@link #STATUS_SUCCESS}, {@link #STATUS_FATAL},
     *                {@link #STATUS_ERROR}, {@link #STATUS_WARNING}, or {@link #STATUS_RETRY}
     * @param message detail message for the status element content, or {@code null}
     * @return an XmlDocument containing the status response
     */
    protected XmlDocument createStatusDocument(int level, String message) {
        Element output = createOutputDocument();
        addStatusElement(output, level, message, null);
        return new XmlDocument(output.getOwnerDocument());
    }


    /**
     * Adds a {@code <status>} element to an XDS input or output document.
     *
     * @param parent  the {@code <input>} or {@code <output>} element to append to
     * @param level   one of {@link #STATUS_SUCCESS}, {@link #STATUS_FATAL},
     *                {@link #STATUS_ERROR}, {@link #STATUS_WARNING}, or {@link #STATUS_RETRY}
     * @param message detail message for the status element content, or {@code null}
     * @param eventId the event ID this status corresponds to, or {@code null}
     * @return the created {@code <status>} element
     */
    protected Element addStatusElement(Element parent, int level, String message, String eventId) {
        Document document = parent.getOwnerDocument();
        Element status = document.createElementNS(null, "status");
        parent.appendChild(status);
        status.setAttributeNS(null, "level", STATUS_LEVELS[level]);
        if (eventId != null && eventId.length() > 0)
        {
            status.setAttributeNS(null, "event-id", eventId);
        }
        if (message != null && message.length() > 0)
        {
            Text msg = document.createTextNode(message);
            status.appendChild(msg);
        }
        return status;
    }


    /**
     * Creates a bare-bones XDS document containing only the root {@code <nds>} element
     * with {@code dtdversion="4.0"}.
     *
     * @return the root {@code <nds>} element
     */
    protected Element createXdsDocument() {
        Document returnDoc = com.novell.xml.dom.DocumentFactory.newDocument();
        Element nds = returnDoc.createElementNS(null, "nds");
        returnDoc.appendChild(nds);
        nds.setAttributeNS(null, "dtdversion", "4.0");
        return nds;
    }


    /**
     * Returns the association value for a command element (add, modify, delete, etc.)
     * by searching its immediate children for an {@code <association>} element.
     *
     * @param command the command element to search
     * @return the association value as a string, or {@code null} if not found
     */
    protected String getAssociation(Element command) {
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
     * Creates an empty XDS {@code <output>} document for returning data to DirXML.
     *
     * @return the {@code <output>} element
     */
    protected Element createOutputDocument() {
        Element nds = createXdsDocument();
        Element output = nds.getOwnerDocument().createElementNS(null, "output");
        nds.appendChild(output);
        return output;
    }


    /**
     * Creates an empty XDS {@code <input>} document for submitting data
     * (publishing or subscriber query) to DirXML.
     *
     * @return the {@code <input>} element
     */
    protected Element createInputDocument() {
        Element nds = createXdsDocument();
        Element input = nds.getOwnerDocument().createElementNS(null, "input");
        nds.appendChild(input);
        return input;
    }

    /**
     * Extracts the {@code event-id} attribute from an event element.
     *
     * @param eventElement the XDS event element (add, modify, delete, etc.)
     * @return the event ID string
     */
    public String getEventID(Element eventElement) {
        return eventElement.getAttribute("event-id");
    }


    /**
     * Adds a state element to an XDS document for persisting shim-specific state
     * information into NDS.
     * <p>
     * This creates or locates the {@code <init-params>} and state container elements
     * ({@code <driver-state>}, {@code <subscriber-state>}, or {@code <publisher-state>}),
     * then appends a child element with the specified name and value.
     *
     * @param parent       the {@code <input>} or {@code <output>} element
     * @param stateName    one of "driver-state", "subscriber-state", or "publisher-state"
     * @param elementName  the name of the shim-specific state element to create
     * @param elementValue the text content for the state element, or {@code null}
     * @return the created state element
     */
    protected Element addState(Element parent, String stateName, String elementName, String elementValue) {
        Document document = parent.getOwnerDocument();
        Element stateElement;
        Element initParams = (Element) parent.getElementsByTagNameNS(null, "init-params").item(0);
        if (initParams == null)
        {
            initParams = document.createElementNS(null, "init-params");
            parent.appendChild(initParams);
            stateElement = document.createElementNS(null, stateName);
            initParams.appendChild(stateElement);
        } else
        {
            stateElement = (Element) initParams.getElementsByTagNameNS(null, stateName).item(0);
            if (stateElement == null)
            {
                stateElement = document.createElementNS(null, stateName);
                initParams.appendChild(stateElement);
            }
        }
        Element element = document.createElementNS(null, elementName);
        stateElement.appendChild(element);
        if (elementValue != null && elementValue.length() > 0)
        {
            Text value = document.createTextNode(elementValue);
            element.appendChild(value);
        }
        return element;
    }


    /**
     * Extracts authentication parameters from an initialization document.
     * <p>
     * Reads the {@code <authentication-info>} element within {@code <init-params>}
     * to populate the authentication ID (user), authentication context (server/address),
     * and application password. These correspond to the fields on the DirXML-Driver
     * object properties dialog under Authentication.
     *
     * @param initDocument the XML init document passed to a shim {@code init()} method
     * @return an {@link AuthenticationParams} instance; any field may be {@code null}
     *         if not present in the init document
     */
    protected AuthenticationParams getAuthenticationParams(Document initDocument) {
        AuthenticationParams params = new AuthenticationParams();
        Element initParams = (Element) initDocument.getElementsByTagNameNS(null, "init-params").item(0);
        if (initParams == null)
        {
            return params;
        }
        Element authInfo = (Element) initDocument.getElementsByTagNameNS(null, "authentication-info").item(0);
        if (authInfo == null)
        {
            return params;
        }
        Element server = (Element) authInfo.getElementsByTagNameNS(null, "server").item(0);
        if (server != null)
        {
            params.authenticationContext = com.novell.xsl.util.Util.getXSLStringValue(server);
        }
        Element user = (Element) authInfo.getElementsByTagNameNS(null, "user").item(0);
        if (user != null)
        {
            params.authenticationId = com.novell.xsl.util.Util.getXSLStringValue(user);
        }
        Element password = (Element) authInfo.getElementsByTagNameNS(null, "password").item(0);
        if (password != null)
        {
            params.applicationPassword = com.novell.xsl.util.Util.getXSLStringValue(password);
        }
        return params;
    }


    /**
     * Extracts shim option parameters from the options and state elements
     * in an initialization document.
     * <p>
     * Searches for {@code <driver-options>}, {@code <subscriber-options>}, or
     * {@code <publisher-options>} (depending on {@code shimName}), plus the
     * corresponding state element, and populates a {@link ShimParams} object
     * with the values found.
     *
     * @param initDocument the init document passed to the shim {@code init()} method
     * @param shimName     one of "driver", "subscriber", or "publisher"
     * @param paramDesc    array of {@link ShimParamDesc} describing expected parameters
     * @return a {@link ShimParams} object containing the extracted values
     * @throws IllegalArgumentException if a required parameter is missing or has an invalid type
     */
    protected ShimParams getShimParams(Document initDocument, String shimName, ShimParamDesc[] paramDesc)
            throws IllegalArgumentException {
        int i;
        ShimParams params = new ShimParams();
        String optionsName = shimName + "-options";
        tracer.trace("Looking for options element: " + optionsName, 3);
        tracer.trace(initDocument);
        Element optionsElement = (Element) initDocument.getElementsByTagNameNS(null, optionsName).item(0);

        tracer.trace("optionsElement: " + optionsElement);
        extractValues(optionsElement, params, paramDesc);
        String stateName = shimName + "-state";
        Element stateElement = (Element) initDocument.getElementsByTagNameNS(null, stateName).item(0);
        extractValues(stateElement, params, paramDesc);
        StringBuffer errorList = new StringBuffer(128);
        int errorCount = 0;
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
     * Extracts parameter values from an options or state XML element based on
     * the provided parameter descriptors.
     *
     * @param optionsElement the options or state element to extract from, or {@code null}
     * @param params         the {@link ShimParams} to populate
     * @param paramDesc      array of {@link ShimParamDesc} describing expected parameters
     * @throws IllegalArgumentException if an integer-typed parameter has non-numeric content
     */
    private void extractValues(Element optionsElement, ShimParams params, ShimParamDesc[] paramDesc) {
        if (optionsElement != null)
        {
            int i;
            Element option;
            for (i = 0; i < paramDesc.length; ++i)
            {
                option = (Element) optionsElement.getElementsByTagNameNS(null, paramDesc[i].paramName).item(0);
                if (option != null)
                {
                    String content = com.novell.xsl.util.Util.getXSLStringValue(option);
                    if (content == null || content.length() == 0)
                    {
                        continue;
                    }
                    if (paramDesc[i].paramType == ShimParamDesc.STRING_TYPE)
                    {
                        params.putStringParam(paramDesc[i].paramName, content);
                        tracer.trace("Param: " + paramDesc[i].paramName + " :" + content);
                    } else
                    {
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
            }
        } else
        {
            tracer.trace("options element was null");
        }
    }

    /**
     * Sets the driver RDN from the {@code src-dn} attribute of the {@code <init-params>}
     * element. Only sets the value once (on first call with a valid document).
     *
     * @param doc the initialization document containing {@code <init-params>}
     */
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

    /**
     * Reads the publisher heartbeat interval from the {@code <publisher-options>}
     * element of an initialization document.
     *
     * @param initDoc the initialization document
     * @return the heartbeat interval in milliseconds, 0 if not configured,
     *         or -1 if the value is invalid or exceeds {@link Integer#MAX_VALUE}
     */
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
     * Holds authentication parameters extracted from a DirXML initialization document.
     * <p>
     * These correspond to the fields on the DirXML-Driver object properties dialog
     * under Authentication:
     * <ul>
     *   <li><b>Authentication ID</b> — user ID for connecting to the application</li>
     *   <li><b>Authentication context</b> — server name, IP address, or connection string</li>
     *   <li><b>Application password</b> — securely stored application password</li>
     * </ul>
     */
    static class AuthenticationParams {

        /** The authentication user ID (e.g. PostgreSQL username). */
        public String authenticationId = null;
        /** The authentication context (e.g. {@code localhost:5432/idmEvent}). */
        public String authenticationContext = null;
        /** The application password. */
        public String applicationPassword = null;
    }


    /**
     * Describes a single expected parameter in a driver/subscriber/publisher options element.
     * Used by {@link #getShimParams} to know which elements to look for and how to interpret them.
     */
    static class ShimParamDesc {

        /** Indicates the parameter content should be interpreted as a string. */
        public static final int STRING_TYPE = 0;

        /** Indicates the parameter content should be interpreted as an integer. */
        public static final int INT_TYPE = 1;

        /** The XML element name of the parameter. */
        public String paramName;
        /** The expected type: {@link #STRING_TYPE} or {@link #INT_TYPE}. */
        public int paramType;
        /** Whether this parameter is required. If {@code true} and missing, an exception is thrown. */
        public boolean required;


        /**
         * Constructs a parameter descriptor.
         *
         * @param name     the XML element name of the parameter
         * @param type     {@link #STRING_TYPE} or {@link #INT_TYPE}
         * @param required {@code true} if this parameter must be present
         */
        public ShimParamDesc(String name, int type, boolean required) {
            paramName = name;
            paramType = type;
            this.required = required;
        }
    }


    /**
     * A key-value store for driver/subscriber/publisher option values extracted
     * from initialization documents.
     * <p>
     * Values are keyed by the XML element name and may be either strings or integers.
     */
    public static class ShimParams {

        private final Map<String, Object> paramMap = new HashMap<>();


        /**
         * Stores a string parameter value.
         *
         * @param name  the parameter name (XML element name)
         * @param value the parameter value
         */
        public void putStringParam(String name, String value) {
            paramMap.put(name, value);
        }


        /**
         * Retrieves a string parameter value.
         *
         * @param name the parameter name
         * @return the value, or {@code null} if not found
         */
        public String getStringParam(String name) {
            return (String) paramMap.get(name);
        }


        /**
         * Stores an integer parameter value.
         *
         * @param name  the parameter name (XML element name)
         * @param value the parameter value
         */
        public void putIntParam(String name, int value) {
            paramMap.put(name, Integer.valueOf(value));
        }


        /**
         * Retrieves an integer parameter value.
         *
         * @param name the parameter name
         * @return the value, or {@code -1} if not found
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
         * Checks whether a parameter with the given name exists in the collection.
         *
         * @param name the parameter name
         * @return {@code true} if the parameter exists
         */
        public boolean haveParam(String name) {
            return paramMap.containsKey(name);
        }
    }
}
