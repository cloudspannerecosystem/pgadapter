# Google Cloud Spanner PostgreSQL Adapter

## Introduction
Spanner-PGAdapter is a simple, MITM, forward, non-transparent proxy, which 
translates Postgres wire protocol into the Cloud Spanner equivalent. By running
this proxy locally, any Postgres client (including the SQL command-line client
PSQL) should function seamlessly by simply pointing its outbound port to the 
this proxy's inbound port.

Additionally to translation, this proxy also concerns itself with authentication
and to some extent, connection pooling. Translation for the most part is simply
a transformation of the [PostgreSQL wire protocol](https://www.postgresql.org/docs/8.2/protocol-message-formats.html)
except for some cases concerning PSQL, wherein the query itself is translated.

Simple query mode and extended query mode are supported, and any data type
supported by Spanner is also supported. Items, tables and language not native to
Spanner are not supported, unless otherwise specified.

Though the majority of functionality inherent in most PostgreSQL clients
(including PSQL and JDBC) are included out of the box, the following items are
not supported:
* Functions
* COPY protocol
* Prepared Statement DESCRIBE
* SSL
* PSQL meta-commands not included in this list (i.e.: these are supported):
  * `\d <table>` 
  * `\dt <table>`
  * `\dn <table>`
  * `\di <table>`
  * `\l`


## Usage
The PostgreSQL adapter can be started both as a standalone process as well as an 
in-process server.

### Standalone
1. Build a jar file containing all dependencies by running `mvn package`.
2. Execute `java -jar <jar-file> <options>`.

The following options are required to run the proxy:
  
```    
-p <projectname>
  * The project name where the desired Spanner database is running.
    
-i <instanceid>
  * The instance ID where the desired Spanner database is running.

-d <databasename>
  * The Spanner database name.

-c <credentialspath>
  * The full path for the file containing the service account credentials in JSON 
    format.
  * Do remember to grant the service account sufficient credentials to access the
    database.
```

The following options are optional:
```    
-s <port>
  * The inbound port for the proxy. Defaults to 5432.
   
-a
  * Use authentication when connecting. Currently authentication is not strictly
    implemented in the proxy layer, as it is expected to be run locally, and
    will ignore any connection not stemming from localhost. However, it is a
    useful compatibility option if the PostgreSQL client is set to always 
    authenticate. Note that SSL is not included for the same reason that
    authentication logic is not: since all connections are local, sniffing
    traffic should not generally be a concern.

-q
  * PSQL Mode. Use this option when fronting PSQL. This option will incur some
    performance penalties due to query matching and translation and as such is
    not recommended for production. It is further not guaranteed to perfectly
    match PSQL logic. Please only use this mode when using PSQL.

-f <POSTGRESQL|SPANNER>
  * The data result format coming back to the client from the proxy. By default,
    this is POSTGRESQL, but you can choose SPANNER format if you do not wish the
    data to be modified and the client used can handle it.

-b
  * Force the server to send data back in binary PostgreSQL format when no
    specific format has been requested. The PostgreSQL wire protocol specifies 
    that the server should send data in text format in those cases. This 
    setting overrides this default and should be used with caution, for example
    for testing purposes, as clients might not accept this behavior. This 
    setting only affects query results in extended query mode. Queries in 
    simple query mode will always return results in text format. If you do not 
    know what extended query mode and simple query mode is, then you should 
    probably not be using this setting.

-j <commandmetadatapath>
  * The full path for a file containing a JSON object to do SQL translation
    based on RegEx replacement. Any item matching the input_pattern will be
    replaced by the output_pattern string, wherein capture groups are allowed and
    their order is specified via the matcher_array item. Match replacement must be
    defined via %s in the output_pattern string. Set matcher_array to [] if no
    matches exist. Alternatively, you may place the matching group names 
    directly within the output_pattern string using matcher.replaceAll() rules
    (that is to say, placing the item within braces, preceeeded by a dollar sign);
    For this specific case, matcher_array must be left empty. User-specified 
    patterns will precede internal matches. Escaped and general regex syntax 
    matches Java RegEx syntax; more information on the Java RegEx syntax found 
    here: 
    https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
    * Example:
        { 
          "commands": 
            [ 
              {
                "input_pattern": "^SELECT * FROM users;$", 
                "output_pattern": "SELECT 1;",
                "matcher_array": []
              }, 
              {
                "input_pattern": "^ab(?<firstgroup>\\d)(?<secondgroup>\\d)$",
                "output_pattern": "second number: %s, first number: %s",
                "matcher_array": ["secondgroup", "firstgroup"] 
              },
              {
                "input_pattern": "^cd(?<firstgroup>\\d)(?<secondgroup>\\d)$",
                "output_pattern": "second number: ${secondgroup}, first number: ${firstgroup}",
                "matcher_array": [] 
              }
            ]
        }
        
        
        Input queries:
        "SELECT * FROM users;"
        "ab12"
        "cd34"

        Output queries:
        "SELECT 1;"
        "second number: 2, first number: 1"
        "second number: 4, first number: 3"
```
An example of a simple run string:

``` 
java -jar <jar-file> -p <project name> -i <instance id> -d <database name> -c
<path to credentials file> -s 5432 
```

### In-process
1. Add google-cloud-spanner-pgadapter as a dependency to your project.
2. Build a server using the `com.google.cloud.spanner.pgadapter.ProxyServer` 
class:

```java
class PGProxyRunner {
    public static void main() {
        ProxyServer server = new ProxyServer(
          new OptionsMetadata(
                "jdbc:cloudspanner:/projects/my-project-name"
                + "/instances/my-instance-id"
                + "/databases/my-database-name"
                + ";credentials=/home/user/service-account-credentials.json",
                portNumber,
                textFormat,
                forceBinary,
                authenticate,
                psqlMode,
                commandMetadataJSON)
        );
        server.startServer();
    }
}
```

Wherein the first item is the JDBC connection string containing pertinent
information regarding project id, instance id, database name, credentials file
path; All other items map directly to previously mentioned CLI options.

## Support Level

Please feel free to report issues and send pull requests, but note that this 
application is not officially supported as part of the Cloud Spanner Product.
