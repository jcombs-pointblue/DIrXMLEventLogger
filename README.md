# DirXML Event Logger Driver

This project implements a skeleton DirXML driver for NetIQ Identity Manager that logs events passing through the system.

## Overview

The DirXML Event Logger Driver is designed to log events that pass through the Identity Manager system. It implements the SubscriptionShim interface to capture and log events from the subscriber channel.

## Components

### EventLoggerDriver

The main driver class that implements the DriverShim interface. It initializes and manages the driver lifecycle.

Key methods:
- `init(XmlDocument)`: Initializes the driver
- `shutdown(XmlDocument)`: Shuts down the driver
- `getSubscriptionShim()`: Returns the subscription shim implementation
- `getPublicationShim()`: Returns null (not implemented in this driver)
- `getSchema(XmlDocument)`: Returns schema information for the driver

### SubscriptionShimImpl

Implements the SubscriptionShim interface to handle events from the subscriber channel.

Key methods:
- `init(XmlDocument)`: Initializes the subscription shim
- `subscribe(XmlDocument)`: Processes documents from the subscriber channel
- `execute(XmlDocument, XmlQueryProcessor)`: Executes queries on the subscription shim

## Future Enhancements

As noted in the Notes.txt file, future enhancements could include:

1. Logging events to a file that rotates as needed
2. Logging to a relational database
3. Logging to an Event database
4. Logging to a message queue
5. Ability to key off of src-dn, dest-dn, association, event type
6. Implementation of a way for other drivers to log input and output documents
7. Adding debugging of engine, startup parameters, memory logging, etc.

## Installation

To install the driver:

1. Compile the Java classes
2. Package them into a JAR file
3. Deploy the JAR file to the Identity Manager server
4. Configure the driver in the Identity Manager administration console

## Configuration

The driver can be configured through the Identity Manager administration console. Configuration options will include:

- Log destination (file, database, event DB, message queue)
- Log rotation settings (if logging to a file)
- Event filtering options
- Debug level