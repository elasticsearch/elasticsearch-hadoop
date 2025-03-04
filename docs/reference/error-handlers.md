---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/errorhandlers.html
navigation_title: Error handlers
---
# {{esh-full}} error handlers

Elasticsearch for Apache Hadoop is designed to be a mostly hands off integration. Most of the features are managed through conventions and configurations and no substantial amount of code is required to get up and running with the connector. When it comes to exceptions, substantial effort has been put into handling the most common and expected errors from {{es}}. In the case where errors are unexpected or indicative of a real problem, the connector adopts a "fail-fast" approach. We realize that this approach is not the best for all users, especially those concerned with uptime of their jobs.

In these situations, users can handle unexpected errors by specifying the actions to take when encountering them. To this end, we have provided a set of APIs and extensions that users can implement in order to tailor the connector’s behavior toward encountering failures in the most common locations to their needs.

## Error handler mechanics [errorhandlers-mechanics]

Each type of failure that can be handled in elasticsearch-hadoop has its own error handler API tailored to operation being performed. An error handler is simply a function that is called when an error occurs, and informs elasticsearch-hadoop how to proceed. Multiple error handlers may be specified for each type of failure. When a failure occurs, each error handler will be executed in the order that they are specified in the configuration.

An error handler is given information about the performed operation and any details about the error that was encountered. The handler may then acknowledge and consume the failure telling elasticsearch-hadoop to ignore the error. Alternatively, the handler may mark the error to be rethrown, potentially ending the job. Error handlers are also given the option to modify the operation’s parameters and retry it. Finally, the handler may also "pass" the failure on to the next handler in the list if it does not know how to handle it.

If every handler in the provided list of handlers chooses to "pass", it is marked as an unhandled error and the exceptions will be rethrown, potentially ending the job. The connector ships with a few default error handlers that take care of most use cases, but if you find that you need a more specific error handling strategy, you can always write your own.

In the following sections, we will detail the different types of error handlers, where they are called, how to configure them, and how to write your own if you need to.

## Bulk write error handlers [errorhandlers-bulk]

When writing data, the connector batches up documents into a bulk request before sending them to {{es}}. In the response, {{es}} returns a status for each document sent, which may include rejections or failures. A common error encountered here is a *rejection* which means that the shard that the document was meant to be written to was too busy to accept the write. Other failures here might include documents that are refused because they do not conform to the current index mapping, or conflict with the current version of the document.

Elasticsearch for Apache Hadoop provides an API to handle document level errors from bulk responses. Error handlers for bulk writes are given:

* The raw JSON bulk entry that was tried
* Error message
* HTTP status code for the document
* Number of times that the current document has been sent to {es}

There are a few default error handlers provided by the connector:

### HTTP Retry Handler [errorhandlers-bulk-http]

Always configured as the first error handler for bulk writes.

This handler checks the failures for common HTTP codes that can be retried. These codes are usually indicators that the shard the document would be written to is too busy to accept the write, and the document was rejected to shed load. This handler is always configured to be run first for bulk failures. All handlers that are configured by the user are placed in order after this one.

While the handler’s position in the list of error handlers cannot be modified, its behavior can be modified by adjusting the following configurations:

`es.batch.write.retry.policy` (default: simple)
:   Defines the policy for determining which http codes are able to be retried. The default value `simple` allows for 429 (Too Many Requests) and 503 (Unavailable) to be retried. Setting the value to `none` will allow no status codes to be retried.

`es.batch.write.retry.count` (default 3)
:   Number of retries for a given batch in case {{es}} is overloaded and data is rejected. Note that only the rejected data is retried. If there is still data rejected after the retries have been performed, the Hadoop job is cancelled (and fails). A negative value indicates infinite retries; be careful in setting this value as it can have unwanted side effects.

`es.batch.write.retry.wait` (default 10s)
:   Time to wait between batch write retries that are caused by bulk rejections.


#### Drop and Log Error Handler [errorhandlers-bulk-log]

Default Handler Name: `log`

When this handler is invoked it logs a message containing the JSON bulk entry that failed, the error message, and any previous handler messages. After logging this message, the handler signals that the error has been acknowledged, thus consuming/ignoring it.

Available configurations for this handler:

`es.write.rest.error.handler.log.logger.name` (required)
:   The string name to use when creating the logger instance to log the errors. This setting is required if this handler is used.

`es.write.rest.error.handler.log.logger.class` (alternative to logger.name)
:   The class name to use when creating the logger instance to log the errors. This setting can be used instead of the required setting `es.write.rest.error.handler.log.logger.name`.

`es.write.rest.error.handler.log.logger.level` (default: WARN)
:   The logger level to use when logging the error message. Available options are `FATAL`, `ERROR`, `WARN`, `INFO`, `DEBUG`, and `TRACE`.


#### Abort on Failure Error Handler [errorhandlers-bulk-fail]

Default Handler Name: `fail`

When this handler is called it rethrows the error given to it and aborts. This handler is always loaded and automatically placed at the end of the list of error handlers.

There are no configurations for this handler.


#### Elasticsearch Error Handler [errorhandlers-bulk-es]

Default Handler Name: `es`

When this handler is invoked it converts the error given into a JSON document and inserts it into an Elasticsearch index. When this indexing operation is complete, the handler can be configured to signal that the error is handled (default), or pass the error along to the next handler in the chain. The ES handler serializes error information using its own mapping of the Elastic Common Schema. This data can accept additional user provided metadata that can assist users in searching and reporting on failures (see labels and tags settings below).

::::{note}
Error handlers are built to execute on a per record/error basis. The Elasticsearch Error Handler does not bulk insert error information currently. Each error is inserted one at a time in order to ensure each record/error is handled. This handler may not provide a very high throughput in cases where there are a large number of error events to handle. You should try to design your jobs so that error handling is a relatively uncommon occurrence.
::::


Available configurations for this handler:

`es.write.rest.error.handler.es.client.nodes` (default: "localhost" or currently configured nodes)
:   The comma separated string of node addresses to write errors to. It is recommended that this be a different cluster than the one being written to (in order to avoid write contention).

`es.write.rest.error.handler.es.client.port` (default: 9200 or currently configured port)
:   The http port to use when connecting to the Elasticsearch nodes.

`es.write.rest.error.handler.es.client.resource` (required)
:   The index to write error information into. It is highly recommended that this index be just for error information. Does not support index patterns.

`es.write.rest.error.handler.es.client.inherit` (default: true)
:   Determines if the settings to create the client used for sending error information should inherit the same client settings from the currently running job. By default, all client settings in the job configuration are inherited by this client.

`es.write.rest.error.handler.es.client.conf.<CONFIGURATION>`
:   This configuration prefix is used to set client configuration values in the handler’s underlying ES client. This accepts most of the settings documented in [*Configuration*](/reference/configuration.md).

`es.write.rest.error.handler.es.label.<LABEL>` (optional)
:   A user defined label field that is added to each error event created by this handler. This field will be indexed into the Elasticsearch index provided for this handler. Text data only.

`es.write.rest.error.handler.es.tags` (optional)
:   The comma separated string of tags to add to each error event created by this handler. This field will be indexed into the Elasticsearch index provided for this handler. Text data only.

`es.write.rest.error.handler.es.return.default` (default: HANDLED)
:   The handler result to be returned to the error handling framework when an error is successfully written to Elasticsearch. Available values are `HANDLED`, `PASS`, and `ABORT`. Default result is `HANDLED`.

`es.write.rest.error.handler.es.return.default.reason` (optional)
:   In the case that the default return value is `PASS`, this optional text setting allows a user to specify the reason for the handler to pass the data along to the next handler in the chain.

`es.write.rest.error.handler.es.return.error` (default: ABORT)
:   The handler result to be returned to the error handling framework when an error cannot be written to Elasticsearch. Available values are `HANDLED`, `PASS`, and `ABORT`. Default result is `ABORT`.

`es.write.rest.error.handler.es.return.error.reason` (optional)
:   In the case that the error return value is `PASS`, this optional text setting allows a user to specify the reason for the handler to pass the data along to the next handler in the chain.


### Using Bulk Error Handlers [errorhandlers-bulk-use]

To configure bulk error handlers, you must specify the handlers in order with the following properties.

Setting `es.write.rest.error.handlers`
:   Lists the names of the error handlers to use for bulk write error handling, and the order that they should be called on. Each default handler can be referenced by their handler name as the connector knows how to load them. Any handlers provided from users or third party code will need to have their handler names defined with the `es.write.rest.error.handler.` prefix.

For bulk write failures, the HTTP Retry built-in handler is always placed as the first error handler. Additionally, the Abort on Failure built-in handler is always placed as the last error handler to catch any unhandled errors. These two error handlers alone form the default bulk write error handling behavior for elasticsearch-hadoop, which matches the behavior from previous versions.

1. HTTP Retry Built-In Handler: Retries benign bulk rejections and failures from {{es}} and passes any other error down the line
2. Any configured user handlers will go here.
3. Abort on Failure Built-In Handler: Rethrows the any errors it encounters

This behavior is modified by inserting handlers into the chain by using the handlers property. Let’s say that we want to log ALL errors and ignore them.

```ini
es.write.rest.error.handlers = log <1>
```

1. Specifying the default Drop and Log handler


With the above configuration, the handler list now looks like the following:

1. HTTP Retry Handler
2. Drop and Log Handler
3. Abort on Failure Handler

As described above, the built-in `log` error handler has a required setting: What to use for the logger name. The logger used will respect whatever logging configuration you have in place, and thus needs a name for the logger to use:

```ini
es.write.rest.error.handlers = log <1>
es.write.rest.error.handler.log.logger.name = BulkErrors <2>
```

1. Specifying the default Drop and Log built-in handler
2. The Drop and Log built-in handler will log all errors to the `BulkErrors` logger


At this point, the Abort on Failure built-in handler is effectively ignored since the Drop and Log built-in handler will always mark an error as consumed. This practice can prove to be hazardous, as potentially important errors may simply be ignored. In many cases, it is preferable for users to write their own error handler to handle expected exceptions.


#### Writing Your Own Bulk Error Handlers [errorhandlers-bulk-user-handlers]

Let’s say that you are streaming sensitive transaction data to {{es}}. In this scenario, your data is carefully versioned and you take advantage of {{es}}'s version system to keep from overwriting newer data with older data. Perhaps your data is distributed in a way that allows newer data to sneak in to {{es}} before some older bits of data. No worries, the version system will reject the older data and preserve the integrity of the data in {{es}}. The problem here is that your streaming job has failed because conflict errors were returned and the connector was unsure if you were expecting that.

Let’s write an error handler for this situation:

```java
package org.myproject.myhandlers;

import org.elasticsearch.hadoop.handler.HandlerResult;
import org.elasticsearch.hadoop.rest.bulk.handler.BulkWriteErrorHandler;
import org.elasticsearch.hadoop.rest.bulk.handler.BulkWriteFailure;
import org.elasticsearch.hadoop.rest.bulk.handler.DelayableErrorCollector;

public class IgnoreConflictsHandler extends BulkWriteErrorHandler { <1>

    private static final Logger LOGGER = ...; <2>

    @Override
    public HandlerResult onError(BulkWriteFailure entry, DelayableErrorCollector<byte[]> collector) <3>
    throws Exception
    {
        if (entry.getResponseCode() == 409) { <4>
            LOGGER.warn("Encountered conflict response. Ignoring old data.");
            return HandlerResult.HANDLED; <5>
        }
        return collector.pass("Not a conflict response code."); <6>
    }
}
```

1. We create a class and extend the BulkWriteErrorHandler base class
2. Create a logger using preferred logging solution
3. Override the `onError` method which will be invoked with the error details
4. Check the response code from the error to see if it is 409 (Confict)
5. If it is a conflict, log the error and return `HandlerResult.HANDLED` to signal that the error is acknowledged
6. If the error is not a conflict we pass it along to the next error handler with the reason we couldn’t handle it


Before we can place this handler in the list of bulk write error handlers, we must register the handler class with a name in the settings using `es.write.rest.error.handler.[HANDLER-NAME]`:

Setting `es.write.rest.error.handler.[HANDLER-NAME]`
:   Create a new handler named HANDLER-NAME. The value of this property must be the binary name of the class to instantiate for this handler.

In this case, lets register a handler name for our ignore conflicts handler:

```ini
es.write.rest.error.handler.ignoreConflict = org.myproject.myhandlers.IgnoreConflictsHandler
```

Now that we have a name for the handler, we can use it in the handler list:

```ini
es.write.rest.error.handlers = ignoreConflict
es.write.rest.error.handler.ignoreConflict = org.myproject.myhandlers.IgnoreConflictsHandler
```

Now, your ignore conflict error handler will be invoked whenever a bulk failure occurs, and will instruct the connector that it is ok with ignoring conflict response codes from {{es}}.


#### Advanced Concepts [errorhandlers-bulk-advanced]

What if instead of logging data and dropping it, what if you wanted to persist it somewhere for safe keeping? What if we wanted to pass properties into our handlers to parameterize their behavior? Lets create a handler that stores error information in a local file for later analysis.

```java
package org.myproject.myhandlers;

import ...

import org.elasticsearch.hadoop.handler.HandlerResult;
import org.elasticsearch.hadoop.rest.bulk.handler.BulkWriteErrorHandler;
import org.elasticsearch.hadoop.rest.bulk.handler.BulkWriteFailure;
import org.elasticsearch.hadoop.rest.bulk.handler.DelayableErrorCollector;

public class OutputToFileHandler extends BulkWriteErrorHandler { <1>

    private OutputStream outputStream;   <2>
    private BufferedWriter writer;

    @Override
    public void init(Properties properties) {   <3>
        try {
            outputStream = new FileOutputStream(properties.getProperty("filename"));   <4>
            writer = new BufferedWriter(new OutputStreamWriter(outputStream));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Could not open file", e);
        }
    }

    @Override
    public HandlerResult onError(BulkWriteFailure entry, DelayableErrorCollector<byte[]> collector)   <5>
    throws Exception
    {
        writer.write("Code: " + entry.getResponseCode());
        writer.newLine();
        writer.write("Error: " + entry.getException().getMessage());
        writer.newLine();
        for (String message : entry.previousHandlerMessages()) {
            writer.write("Previous Handler: " + message);           <6>
            writer.newLine();
        }
        writer.write("Attempts: " + entry.getNumberOfAttempts());
        writer.newLine();
        writer.write("Entry: ");
        writer.newLine();
        IOUtils.copy(entry.getEntryContents(), writer);
        writer.newLine();

        return HandlerResult.HANDLED; <7>
    }

    @Override
    public void close() {   <8>
        try {
            writer.close();
            outputStream.close();
        } catch (IOException e) {
            throw new RuntimeException("Closing file failed", e);
        }
    }
}
```

1. Extend the BulkWriteErrorHandler base class
2. Some local state for writing data out to a file
3. We override the `init` method. Any properties for this handler are passed in here.
4. We are extracting the file to write to from the properties. We’ll see how to set this property below.
5. Overriding the `onError` method to define our behavior.
6. Write out the error information. This highlights all the available data provided by the `BulkWriteFailure` object.
7. Return the `HANDLED` result to signal that the error is handled.
8. Finally, close out any internally allocated resources.


Added to this handler are the `init` and `close` methods. The `init` method is called when the handler is first created at the start of the task and the `close` method is called when the task concludes. The `init` method accepts a properties parameter, which contains any handler specific properties set by using `es.write.rest.error.handler.[HANDLER-NAME].[PROPERTY-NAME]`.

Setting `es.write.rest.error.handler.[HANDLER-NAME].[PROPERTY-NAME]`
:   Used to pass properties into handlers. HANDLER-NAME is the handler to be configured, and PROPERTY-NAME is the property to set for the handler.

In our use case, we will configure the our file logging error handler like so:

```ini
es.write.rest.error.handler.writeFile = org.myproject.myhandlers.OutputToFileHandler   <1>
es.write.rest.error.handler.writeFile.filename = /path/to/some/output/file   <2>
```

1. We register our new handler with the name `writeFile`
2. Now we set a property named `filename` for the `writeFile` handler. In the `init` method of the handler, this can be picked up by using `filename` as the property key.


Now to bring it all together with the previous example (ignoring conflicts):

```ini
es.write.rest.error.handlers = ignoreConflict,writeFile

es.write.rest.error.handler.ignoreConflict = org.myproject.myhandlers.IgnoreConflictsHandler

es.write.rest.error.handler.writeFile = org.myproject.myhandlers.OutputToFileHandler
es.write.rest.error.handler.writeFile.filename = /path/to/some/output/file
```

You now have a chain of handlers that retries bulk rejections by default (HTTP Retry built-in handler), then ignores any errors that are conflicts (our own ignore conflicts handler), then ignores any other errors by writing them out to a file (our own output to file handler).


## Serialization Error Handlers [errorhandlers-serialization]

Before sending data to Elasticsearch, elasticsearch-hadoop must serialize each document into a JSON bulk entry. It is during this process that the bulk operation is determined, document metadata is extracted, and integration specific data structures are converted into JSON documents. During this process, inconsistencies with record structure can cause exceptions to be thrown during the serialization process. These errors often lead to failed tasks and halted processing.

Elasticsearch for Apache Hadoop provides an API to handle serialization errors at the record level. Error handlers for serialization are given:

* The integration specific data structure that was unable to be serialized
* Exception encountered during serialization

::::{note}
Serialization Error Handlers are not yet available for Hive. Elasticsearch for Apache Hadoop uses Hive’s SerDe constructs to convert data into bulk entries before being sent to the output format. SerDe objects do not have a cleanup method that is called when the object ends its lifecycle. Because of this, we do not support serialization error handlers in Hive as they cannot be closed at the end of the job execution.
::::

There are a few default error handlers provided by the connector:

### Drop and Log Error Handler [errorhandlers-serialization-log]

Default Handler Name: `log`

When this handler is invoked it logs a message containing the data structure’s toString() contents that failed, the error message, and any previous handler messages. After logging this message, the handler signals that the error has been acknowledged, thus consuming/ignoring it.

Available configurations for this handler:

`es.write.data.error.handler.log.logger.name` (required)
:   The string name to use when creating the logger instance to log the errors. This setting is required if this handler is used.

`es.write.data.error.handler.log.logger.class` (alternative to logger.name)
:   The class name to use when creating the logger instance to log the errors. This setting can be used instead of the required setting `es.write.data.error.handler.log.logger.name`.

`es.write.data.error.handler.log.logger.level` (default: WARN)
:   The logger level to use when logging the error message. Available options are `FATAL`, `ERROR`, `WARN`, `INFO`, `DEBUG`, and `TRACE`.


### Abort on Failure Error Handler [errorhandlers-serialization-fail]

Default Handler Name: `fail`

When this handler is called it rethrows the error given to it and aborts. This handler is always loaded and automatically placed at the end of the list of error handlers.

There are no configurations for this handler.


### Elasticsearch Error Handler [errorhandlers-serialization-es]

Default Handler Name: `es`

When this handler is invoked it converts the error given into a JSON document and inserts it into an Elasticsearch index. When this indexing operation is complete, the handler can be configured to signal that the error is handled (default), or pass the error along to the next handler in the chain. The ES handler serializes error information using its own mapping of the Elastic Common Schema. This data can accept additional user provided metadata that can assist users in searching and reporting on failures (see labels and tags settings below).

::::{note}
Error handlers are built to execute on a per record/error basis. The Elasticsearch Error Handler does not bulk insert error information currently. Each error is inserted one at a time in order to ensure each record/error is handled. This handler may not provide a very high throughput in cases where there are a large number of error events to handle. You should try to design your jobs so that error handling is a relatively uncommon occurrence.
::::


Available configurations for this handler:

`es.write.rest.error.handler.es.client.nodes` (default: "localhost" or currently configured nodes)
:   The comma separated string of node addresses to write errors to. It is recommended that this be a different cluster than the one being written to (in order to avoid write contention).

`es.write.rest.error.handler.es.client.port` (default: 9200 or currently configured port)
:   The http port to use when connecting to the Elasticsearch nodes.

`es.write.rest.error.handler.es.client.resource` (required)
:   The index to write error information into. It is highly recommended that this index be just for error information. Does not support index patterns.

`es.write.rest.error.handler.es.client.inherit` (default: true)
:   Determines if the settings to create the client used for sending error information should inherit the same client settings from the currently running job. By default, all client settings in the job configuration are inherited by this client.

`es.write.rest.error.handler.es.client.conf.<CONFIGURATION>`
:   This configuration prefix is used to set client configuration values in the handler’s underlying ES client. This accepts most of the settings documented in [*Configuration*](/reference/configuration.md).

`es.write.rest.error.handler.es.label.<LABEL>` (optional)
:   A user defined label field that is added to each error event created by this handler. This field will be indexed into the Elasticsearch index provided for this handler. Text data only.

`es.write.rest.error.handler.es.tags` (optional)
:   The comma separated string of tags to add to each error event created by this handler. This field will be indexed into the Elasticsearch index provided for this handler. Text data only.

`es.write.rest.error.handler.es.return.default` (default: HANDLED)
:   The handler result to be returned to the error handling framework when an error is successfully written to Elasticsearch. Available values are `HANDLED`, `PASS`, and `ABORT`. Default result is `HANDLED`.

`es.write.rest.error.handler.es.return.default.reason` (optional)
:   In the case that the default return value is `PASS`, this optional text setting allows a user to specify the reason for the handler to pass the data along to the next handler in the chain.

`es.write.rest.error.handler.es.return.error` (default: ABORT)
:   The handler result to be returned to the error handling framework when an error cannot be written to Elasticsearch. Available values are `HANDLED`, `PASS`, and `ABORT`. Default result is `ABORT`.

`es.write.rest.error.handler.es.return.error.reason` (optional)
:   In the case that the error return value is `PASS`, this optional text setting allows a user to specify the reason for the handler to pass the data along to the next handler in the chain.

## Using Serialization Error Handlers [errorhandlers-serialization-use]

To configure serialization error handlers, you must specify the handlers in order with the following properties.

Setting `es.write.data.error.handlers`
:   Lists the names of the error handlers to use for serialization error handling, and the order that they should be called on. Each default handler can be referenced by their handler name as the connector knows how to load them. Any handlers provided from users or third party code will need to have their handler names defined with the `es.write.data.error.handler.` prefix.

For serialization failures, the Abort on Failure built-in handler is always placed as the last error handler to catch any unhandled errors. This error handler forms the default serialization error handling behavior for elasticsearch-hadoop, which matches the behavior from previous versions.

1. Any configured user handlers will go here.
2. Abort on Failure Built-In Handler: Rethrows the any errors it encounters

This behavior is modified by inserting handlers into the chain by using the handlers property. Let’s say that we want to log ALL errors and ignore them.

```ini
es.write.data.error.handlers = log <1>
```

1. Specifying the default Drop and Log handler


With the above configuration, the handler list now looks like the following:

1. Drop and Log Handler
2. Abort on Failure Handler

As described above, the built-in `log` error handler has a required setting: What to use for the logger name. The logger used will respect whatever logging configuration you have in place, and thus needs a name for the logger to use:

```ini
es.write.data.error.handlers = log <1>
es.write.data.error.handler.log.logger.name = SerializationErrors <2>
```

1. Specifying the default Drop and Log built-in handler
2. The Drop and Log built-in handler will log all errors to the `SerializationErrors` logger


At this point, the Abort on Failure built-in handler is effectively ignored since the Drop and Log built-in handler will always mark an error as consumed. This practice can prove to be hazardous, as potentially important errors may simply be ignored. In many cases, it is preferable for users to write their own error handler to handle expected exceptions.


### Writing Your Own Serialization Handlers [errorhandlers-serialization-user-handlers]

Let’s say that you are streaming some unstructured data to {{es}}. In this scenario, your data is not fully sanitized and may contain field values that cannot be translated to JSON by the connector. You may not want to have your streaming job fail on this data, as you are potentially expecting it to contain errors. In this situation, you may want to log the data in a more comprehensive manner than to rely on the logging solution’s toString() method for your data.

Let’s write an error handler for this situation:

```java
package org.myproject.myhandlers;

import org.elasticsearch.hadoop.handler.HandlerResult;
import org.elasticsearch.hadoop.handler.ErrorCollector;
import org.elasticsearch.hadoop.serialization.handler.write.SerializationErrorHandler;
import org.elasticsearch.hadoop.serialization.handler.write.SerializationFailure;

public class CustomLogOnError extends SerializationErrorHandler {      <1>

    private Log logger = ???; <2>

    @Override
    public HandlerResult onError(SerializationFailure entry, ErrorCollector<Object> collector) throws Exception {  <3>
        MyRecord record = (MyRecord) entry.getRecord();                             <4>
        logger.error("Could not serialize record. " +
                "Record data : " + record.getSpecificField() + ", " + record.getOtherField(), entry.getException()); <5>
        return HandlerResult.HANDLED;                                               <6>
    }
}
```

1. We create a class and extend the SerializationErrorHandler base class
2. Create a logger using preferred logging solution
3. Override the `onError` method which will be invoked with the error details
4. Retrieve the record that failed to be serialized. Cast it to the record type you are expecting from your job
5. Log the specific information from the data you are interested in
6. Finally after logging the error, return `HandlerResult.HANDLED` to signal that the error is acknowledged


Before we can place this handler in the list of serialization error handlers, we must register the handler class with a name in the settings using `es.write.data.error.handler.[HANDLER-NAME]`:

Setting `es.write.data.error.handler.[HANDLER-NAME]`
:   Create a new handler named HANDLER-NAME. The value of this property must be the binary name of the class to instantiate for this handler.

In this case, lets register a handler name for our ignore conflicts handler:

```ini
es.write.data.error.handler.customLog = org.myproject.myhandlers.CustomLogOnError
```

Now that we have a name for the handler, we can use it in the handler list:

```ini
es.write.data.error.handlers = customLog
es.write.data.error.handler.customLog = org.myproject.myhandlers.CustomLogOnError
```

Now, your custom logging error handler will be invoked whenever a serialization failure occurs, and will instruct the connector that it is ok with ignoring those failures to continue processing.


### Advanced Concepts [errorhandlers-serialization-advanced]

Instead of logging data and dropping it, what if you wanted to persist it somewhere for safe keeping? What if we wanted to pass properties into our handlers to parameterize their behavior? Lets create a handler that stores error information in a local file for later analysis.

```java
package org.myproject.myhandlers;

import ...

import org.elasticsearch.hadoop.handler.HandlerResult;
import org.elasticsearch.hadoop.handler.ErrorCollector;
import org.elasticsearch.hadoop.serialization.handler.write.SerializationErrorHandler;
import org.elasticsearch.hadoop.serialization.handler.write.SerializationFailure;

public class OutputToFileHandler extends SerializationErrorHandler { <1>

    private OutputStream outputStream;   <2>
    private BufferedWriter writer;

    @Override
    public void init(Properties properties) {   <3>
        try {
            outputStream = new FileOutputStream(properties.getProperty("filename"));   <4>
            writer = new BufferedWriter(new OutputStreamWriter(outputStream));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Could not open file", e);
        }
    }

    @Override
    public HandlerResult onError(SerializationFailure entry, ErrorCollector<Object> collector)   <5>
    throws Exception
    {
        writer.write("Record: " + entry.getRecord().toString());
        writer.newLine();
        writer.write("Error: " + entry.getException().getMessage());
        writer.newLine();
        for (String message : entry.previousHandlerMessages()) {
            writer.write("Previous Handler: " + message);           <6>
            writer.newLine();
        }

        return HandlerResult.PASS; <7>
    }

    @Override
    public void close() {   <8>
        try {
            writer.close();
            outputStream.close();
        } catch (IOException e) {
            throw new RuntimeException("Closing file failed", e);
        }
    }
}
```

1. Extend the SerializationErrorHandler base class
2. Some local state for writing data out to a file
3. We override the `init` method. Any properties for this handler are passed in here.
4. We are extracting the file to write to from the properties. We’ll see how to set this property below.
5. Overriding the `onError` method to define our behavior.
6. Write out the error information. This highlights all the available data provided by the `SerializationFailure` object.
7. Return the `PASS` result to signal that the error should be handed off to the next error handler in the chain.
8. Finally, close out any internally allocated resources.


Added to this handler are the `init` and `close` methods. The `init` method is called when the handler is first created at the start of the task and the `close` method is called when the task concludes. The `init` method accepts a properties parameter, which contains any handler specific properties set by using `es.write.data.error.handler.[HANDLER-NAME].[PROPERTY-NAME]`.

Setting `es.write.data.error.handler.[HANDLER-NAME].[PROPERTY-NAME]`
:   Used to pass properties into handlers. HANDLER-NAME is the handler to be configured, and PROPERTY-NAME is the property to set for the handler.

In our use case, we will configure the our file logging error handler like so:

```ini
es.write.data.error.handler.writeFile = org.myproject.myhandlers.OutputToFileHandler   <1>
es.write.data.error.handler.writeFile.filename = /path/to/some/output/file <2>
```

1. We register our new handler with the name `writeFile`
2. Now we set a property named `filename` for the `writeFile` handler. In the `init` method of the handler, this can be picked up by using `filename` as the property key.


Now to bring it all together:

```ini
es.write.data.error.handlers = writeFile,customLog

es.write.data.error.handler.customLog = org.myproject.myhandlers.CustomLogOnError

es.write.data.error.handler.writeFile = org.myproject.myhandlers.OutputToFileHandler
es.write.data.error.handler.writeFile.filename = /path/to/some/output/file
```

You now have a chain of handlers that writes all relevant data about the failure to a file (our writeFile handler), then logs the errors using a custom log line and ignores the error to continue processing (our customLog handler).


## Deserialization Error Handlers [errorhandlers-read-json]

When reading data, the connector executes scroll requests against the configured indices and reads their contents. For each hit in a scroll search result, the connector attempts to deserialize it into an integration specific record type. When using MapReduce, this data type is either a MapWritable or Text (for raw JSON data). For an integration like Spark SQL which uses data schemas, the resulting data type is a Row object.

Elasticsearch stores documents in lucene indices. These documents can sometimes have loose definitions, or have structures that cannot be parsed into a schema-based data type, for one reason or another. Sometimes a field may be in a format that cannot be read correctly.

Elasticsearch for Apache Hadoop provides an API to handle document level deserialization errors from scroll responses. Error handlers for scroll reads are given:

* The raw JSON search result that was tried
* Exception encountered

Note: Deserialization Error Handlers only allow handling of errors that occur when parsing documents from scroll responses. It may be possible that a search result can be successfully read, but is still malformed, thus causing an exception when it is used in a completely different part of the framework. This Error Handler is called from the top of the most reasonable place to handle exceptions in the scroll reading process, but this does not encapsulate all logic for each integration.

There are a few default error handlers provided by the connector:


### Drop and Log Error Handler [errorhandlers-read-json-log]

Default Handler Name: `log`

When this handler is invoked it logs a message containing the JSON search hit that failed, the error message, and any previous handler messages. After logging this message, the handler signals that the error has been acknowledged, thus consuming/ignoring it.

Available configurations for this handler:

`es.read.data.error.handler.log.logger.name` (required)
:   The string name to use when creating the logger instance to log the errors. This setting is required if this handler is used.

`es.read.data.error.handler.log.logger.class` (alternative to logger.name)
:   The class name to use when creating the logger instance to log the errors. This setting can be used instead of the required setting `es.read.data.error.handler.log.logger.name`.

`es.read.data.error.handler.log.logger.level` (default: WARN)
:   The logger level to use when logging the error message. Available options are `FATAL`, `ERROR`, `WARN`, `INFO`, `DEBUG`, and `TRACE`.


### Abort on Failure Error Handler [errorhandlers-read-json-fail]

Default Handler Name: `fail`

When this handler is called it rethrows the error given to it and aborts. This handler is always loaded and automatically placed at the end of the list of error handlers.

There are no configurations for this handler.


### Elasticsearch Error Handler [errorhandlers-read-json-es]

Default Handler Name: `es`

When this handler is invoked it converts the error given into a JSON document and inserts it into an Elasticsearch index. When this indexing operation is complete, the handler can be configured to signal that the error is handled (default), or pass the error along to the next handler in the chain. The ES handler serializes error information using its own mapping of the Elastic Common Schema. This data can accept additional user provided metadata that can assist users in searching and reporting on failures (see labels and tags settings below).

::::{note}
Error handlers are built to execute on a per record/error basis. The Elasticsearch Error Handler does not bulk insert error information currently. Each error is inserted one at a time in order to ensure each record/error is handled. This handler may not provide a very high throughput in cases where there are a large number of error events to handle. You should try to design your jobs so that error handling is a relatively uncommon occurrence.
::::


Available configurations for this handler:

`es.write.rest.error.handler.es.client.nodes` (default: "localhost" or currently configured nodes)
:   The comma separated string of node addresses to write errors to. It is recommended that this be a different cluster than the one being written to (in order to avoid write contention).

`es.write.rest.error.handler.es.client.port` (default: 9200 or currently configured port)
:   The http port to use when connecting to the Elasticsearch nodes.

`es.write.rest.error.handler.es.client.resource` (required)
:   The index to write error information into. It is highly recommended that this index be just for error information. Does not support index patterns.

`es.write.rest.error.handler.es.client.inherit` (default: true)
:   Determines if the settings to create the client used for sending error information should inherit the same client settings from the currently running job. By default, all client settings in the job configuration are inherited by this client.

`es.write.rest.error.handler.es.client.conf.<CONFIGURATION>`
:   This configuration prefix is used to set client configuration values in the handler’s underlying ES client. This accepts most of the settings documented in [*Configuration*](/reference/configuration.md).

`es.write.rest.error.handler.es.label.<LABEL>` (optional)
:   A user defined label field that is added to each error event created by this handler. This field will be indexed into the Elasticsearch index provided for this handler. Text data only.

`es.write.rest.error.handler.es.tags` (optional)
:   The comma separated string of tags to add to each error event created by this handler. This field will be indexed into the Elasticsearch index provided for this handler. Text data only.

`es.write.rest.error.handler.es.return.default` (default: HANDLED)
:   The handler result to be returned to the error handling framework when an error is successfully written to Elasticsearch. Available values are `HANDLED`, `PASS`, and `ABORT`. Default result is `HANDLED`.

`es.write.rest.error.handler.es.return.default.reason` (optional)
:   In the case that the default return value is `PASS`, this optional text setting allows a user to specify the reason for the handler to pass the data along to the next handler in the chain.

`es.write.rest.error.handler.es.return.error` (default: ABORT)
:   The handler result to be returned to the error handling framework when an error cannot be written to Elasticsearch. Available values are `HANDLED`, `PASS`, and `ABORT`. Default result is `ABORT`.

`es.write.rest.error.handler.es.return.error.reason` (optional)
:   In the case that the error return value is `PASS`, this optional text setting allows a user to specify the reason for the handler to pass the data along to the next handler in the chain.


## Using Deserialization Error Handlers [errorhandlers-read-json-use]

To configure deserialization error handlers, you must specify the handlers in order with the following properties.

Setting `es.read.data.error.handlers`
:   Lists the names of the error handlers to use for deserialization error handling, and the order that they should be called on. Each default handler can be referenced by their handler name as the connector knows how to load them. Any handlers provided from users or third party code will need to have their handler names defined with the `es.read.data.error.handler.` prefix.

For deserialization failures, the Abort on Failure built-in handler is always placed as the last error handler to catch any unhandled errors. This error handler alone forms the default deserialization error handling behavior for elasticsearch-hadoop, which matches the behavior from previous versions.

1. Any configured user handlers will go here.
2. Abort on Failure Built-In Handler: Rethrows the any errors it encounters

This behavior is modified by inserting handlers into the chain by using the handlers property. Let’s say that we want to log ALL errors and ignore them.

```ini
es.read.data.error.handlers = log <1>
```

1. Specifying the default Drop and Log handler


With the above configuration, the handler list now looks like the following:

1. Drop and Log Handler
2. Abort on Failure Handler

As described above, the built-in `log` error handler has a required setting: What to use for the logger name. The logger used will respect whatever logging configuration you have in place, and thus needs a name for the logger to use:

```ini
es.read.data.error.handlers = log <1>
es.read.data.error.handler.log.logger.name = DeserializationErrors <2>
```

1. Specifying the default Drop and Log built-in handler
2. The Drop and Log built-in handler will log all errors to the `DeserializationErrors` logger


At this point, the Abort on Failure built-in handler is effectively ignored since the Drop and Log built-in handler will always mark an error as consumed. This practice can prove to be hazardous, as potentially important errors may simply be ignored. In many cases, it is preferable for users to write their own error handler to handle expected exceptions.


### Writing Your Own Deserialization Error Handlers [errorhandlers-read-json-user-handlers]

Let’s say that you are reading a large index of log data from {{es}}. In this scenario, your log data is highly unstructured, and not all of its contents are critical to your process. Due to the volume of data being read, your job takes a long time to complete. In this case, you might want to replace records that cannot be read with a dummy record to mark the failure, and not interrupt your processing. The offending data should be logged and dropped.

Let’s write an error handler for this situation:

```java
package org.myproject.myhandlers;

import org.elasticsearch.hadoop.handler.HandlerResult;
import org.elasticsearch.hadoop.handler.ErrorCollector;
import org.elasticsearch.hadoop.serialization.handler.read.DeserializationErrorHandler;
import org.elasticsearch.hadoop.serialization.handler.read.DeserializationFailure;

public class ReturnDummyHandler extends DeserializationErrorHandler { <1>

    private static final Logger LOGGER = ...; <2>
    private static final String DUMMY_RECORD = "..."; <3>

    @Override
    public HandlerResult onError(DeserializationFailure entry, ErrorCollector<byte[]> collector) <4>
    throws Exception
    {
        BufferedReader reader = new BufferedReader(new InputStreamReader(entry.getHitContents()));
        StringBuilder hitContent = new StringBuilder();
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {           <5>
            hitContent.append(line);
        }
        LOGGER.warn("Encountered malformed record during read. Replacing with dummy record. " +   <6>
                            "Malformed Data: " + hitContent, entry.getException());
        return collector.retry(DUMMY_RECORD.getBytes());                                         <7>
    }
}
```

1. We create a class and extend the DeserializationErrorHandler base class
2. Create a logger using preferred logging solution
3. We create a String to use for our dummy record that should be deserialized instead
4. Override the `onError` method which will be invoked with the error details
5. We read the contents of the failed search hit as a String
6. We log the contents of the failed document, as well as the exception that details the cause of the failure
7. Finally, we return the dummy data contents to be deserialized.


Before we can place this handler in the list of deserialization error handlers, we must register the handler class with a name in the settings using `es.read.data.error.handler.[HANDLER-NAME]`:

Setting `es.read.data.error.handler.[HANDLER-NAME]`
:   Create a new handler named HANDLER-NAME. The value of this property must be the binary name of the class to instantiate for this handler.

In this case, lets register a handler name for our dummy record handler:

```ini
es.read.data.error.handler.returnDummy = org.myproject.myhandlers.ReturnDummyHandler
```

Now that we have a name for the handler, we can use it in the handler list:

```ini
es.read.data.error.handlers = returnDummy
es.read.data.error.handler.returnDummy = org.myproject.myhandlers.ReturnDummyHandler
```

Now, your dummy data error handler will be invoked whenever a deserialization failure occurs, and will instruct the connector to use your provided dummy record instead of the malformed data.


### Advanced Concepts [errorhandlers-read-json-advanced]

What if instead of logging data and dropping it, what if you wanted to persist it somewhere for safe keeping? What if we wanted to pass properties into our handlers to parameterize their behavior? Lets create a handler that stores error information in a local file for later analysis.

```java
package org.myproject.myhandlers;

import ...

import org.elasticsearch.hadoop.handler.HandlerResult;
import org.elasticsearch.hadoop.handler.ErrorCollector;
import org.elasticsearch.hadoop.serialization.handler.read.DeserializationErrorHandler;
import org.elasticsearch.hadoop.serialization.handler.read.DeserializationFailure;

public class ReturnDummyAndLogToFileHandler extends DeserializationErrorHandler { <1>

    private static final String DUMMY_RECORD = "...";

    private OutputStream outputStream;   <2>
    private BufferedWriter writer;

    @Override
    public void init(Properties properties) {   <3>
        try {
            outputStream = new FileOutputStream(properties.getProperty("filename"));   <4>
            writer = new BufferedWriter(new OutputStreamWriter(outputStream));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Could not open file", e);
        }
    }

    @Override
    public HandlerResult onError(DeserializationFailure entry, ErrorCollector<byte[]> collector)   <5>
    throws Exception
    {
        BufferedReader reader = new BufferedReader(new InputStreamReader(entry.getHitContents()));
        StringBuilder hitContent = new StringBuilder();
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {           <6>
            hitContent.append(line);
        }

        writer.write("Error: " + entry.getException().getMessage());
        writer.newLine();
        for (String message : entry.previousHandlerMessages()) {
            writer.write("Previous Handler: " + message);           <7>
            writer.newLine();
        }
        writer.write("Entry: ");
        writer.newLine();
        writer.write(hitContent.toString());
        writer.newLine();

        return collector.retry(DUMMY_RECORD.getBytes());            <8>
    }

    @Override
    public void close() {   <9>
        try {
            writer.close();
            outputStream.close();
        } catch (IOException e) {
            throw new RuntimeException("Closing file failed", e);
        }
    }
}
```

1. Extend the DeserializationErrorHandler base class
2. Some local state for writing data out to a file
3. We override the `init` method. Any properties for this handler are passed in here
4. We are extracting the file to write to from the properties. We’ll see how to set this property below
5. Overriding the `onError` method to define our behavior
6. Read the contents of the failed search hit
7. Write out the error information. This highlights all the available data provided by the `DeserializationFailure` object
8. Perform a retry operation, using our dummy record
9. Finally, close out any internally allocated resources


Added to this handler are the `init` and `close` methods. The `init` method is called when the scroll query is first created at the start of the task and the `close` method is called when the scroll query is closed when the task concludes. The `init` method accepts a properties parameter, which contains any handler specific properties set by using `es.read.data.error.handler.[HANDLER-NAME].[PROPERTY-NAME]`.

Setting `es.read.data.error.handler.[HANDLER-NAME].[PROPERTY-NAME]`
:   Used to pass properties into handlers. HANDLER-NAME is the handler to be configured, and PROPERTY-NAME is the property to set for the handler.

In our use case, we will configure the our file logging error handler like so:

```ini
es.read.data.error.handler.writeFile = org.myproject.myhandlers.ReturnDummyAndLogToFileHandler   <1>
es.read.data.error.handler.writeFile.filename = /path/to/some/output/file   <2>
```

1. We register our new handler with the name `writeFile`
2. Now we set a property named `filename` for the `writeFile` handler. In the `init` method of the handler, this can be picked up by using `filename` as the property key.


Now to bring it all together with the previous example:

```ini
es.read.data.error.handlers = writeFile
es.read.data.error.handler.writeFile = org.myproject.myhandlers.ReturnDummyAndLogToFileHandler
es.read.data.error.handler.writeFile.filename = /path/to/some/output/file
```

You now have a handler that retries replaces malformed data with dummy records, then logs those malformed records along with their error information by writing them out to a custom file.


