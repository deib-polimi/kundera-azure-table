#kundera-azure-table

Kundera client module to support Azure Table.

Kundera is _a JPA 2.0 compliant Object-Datastore Mapping Library for NoSQL Datastores_ and is available [here](https://github.com/impetus-opensource/Kundera).

For complete documentation see [Kundera Wiki](https://github.com/impetus-opensource/Kundera/wiki).

##Supported Features
The following feature are supported by this extension:

- JPA relationships are supported as Kundera supports them
- ` @GeneratedValue` only with strategy `GenerationType.AUTO`.
- `@ElementCollection` java `Collection` or `Map` are supported as types and are serialized when persisted into azure table.
- `@Embedded` embedded entities are supported but deeply serialized when persisted into azure table.
- `@Enumerated` java `Enum` types are supported and stored as strings.

For each feature see the relative [JUnit test](https://github.com/Arci/kundera-azure-table/tree/master/src/test/java/it/polimi/kundera/client/azuretable/tests) for usage examples.

##ID and Consistency
In Azure Table __strong consistency__ is guaranteed while entities are stored within the same partition key otherwise consistency will be __eventual__.
IDs are supported only in field of type `String` (so only a `String` field can be annotated with `@Id`).
User can define IDs both with or without partition key.

Please take as reference the naming [constraints](https://msdn.microsoft.com/library/azure/dd179338.aspx) from Azure Table documentation.

###Define both row key and partition key
This can be done in two ways:
- using `AzureTableKey.asString` method by passing both _partition key_ and _row key_ to obtain a string representation of the whole key and assign it to the entity ID field before persist.
- manually define the entity ID before persist the entity, the string must follow the pattern `partitionKey_rowKey`.

###Define only the row key
If only the row key is defined, the _partition key_ is implicitly the default one (which can be set in a [datastore specific properties file](#datastore-specific-properties)).

There are three ways to do this:
- auto-generated IDs (the _row key_ is a random java `UUID`)
- manually define the entity ID before persist the entity
- using `AzureTableKey.asString` passing as parameter the desired _row key_ and assign its result to the entity ID field before persist.

##Query support
JPQL queries are supported as Kundera supports them, the operator supported is resumed in the following table:

| JPA-QL Clause | Azure Table |
|:-------------:|:-----------:|
| SELECT        | &#10004;    |
| UPDATE        | &#10004;    |
| DELETE        | &#10004;    |
| ORDER BY      | X           |
| AND           | &#10004;    |
| OR            | &#10004;    |
| BETWEEN       | &#10004;    |
| LIKE          | X           |
| IN            | X           |
| =             | &#10004;    |
| >             | &#10004;    |
| <             | &#10004;    |
| >=            | &#10004;    |
| <=            | &#10004;    |

Examples in use of queries can be found in the [JUnit test](https://github.com/Arci/kundera-azure-table/blob/master/src/test/java/it/polimi/kundera/client/azuretable/tests/AzureTableQueryTest.java).

More details on the operator supported by Azure Tables can be found in the [official documentation](https://msdn.microsoft.com/en-us/library/azure/dd135725.aspx).

##Configuration

###persistence.xml
The configuration is done in the persistence.xml file, the properties to be specified inside the `<properties>` tag are:

- `kundera.username` __required__, the storage account name (from azure portal)
- `kundera.password` __required__, the storage account key (from azure portal)
- `kundera.client.lookup.class` __required__, `it.polimi.kundera.client.azuretable.AzureTableClientFactory`
- `kundera.ddl.auto.prepare` _optional_, possible values are:
  - `create` which creates the schema (if not already exists)
  - `create-drop` which drop the schema (if exists) and creates it
- `kundera.client.property` _optional_, the name of the xml file containing the datastore specific properties.

###Datastore specific properties
A file with client specific properties can be created and placed inside the classpath, you need to specify its name in the persistence.xml file.

the skeleton of the file is the following:

```
<?xml version="1.0" encoding="UTF-8"?>
<clientProperties>
    <datastores>
        <dataStore>
            <name>azure-table</name>
            <connection>
                <properties>
                    <!-- list of properties -->
                    <property name="" value=""></property>
                </properties>
            </connection>
        </dataStore>
    </datastores>
</clientProperties>
```
for more information see [kundera datastore specific properties](https://github.com/impetus-opensource/Kundera/wiki/Data-store-Specific-Configuration).

The available properties are:

- `table.emulator` [true|false] _default: false_.
If present (and set to `true`) storage emulator is used. When using dev server `kundera.username` and `kundera.password` in persistence xml are ignored.
- `table.emulator.proxy` _default: localhost_.
If storage emulator is used set the value for the emulator proxy.
- `table.protocol` [http|https] _default: https_.
Define the protocol to be used within requests.
- `table.partition.default` _default: DEFAULT_.  
The value for the default partition key, used when no one is specified by the user.
