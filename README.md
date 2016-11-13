# cassandra-zstd
This will allow you to use Zstandard compression in Cassandra 3.x

## Zstandard

Zstandard is a Facebooks efficient opensource compression algorithm that can be located here: [zstd](https://github.com/facebook/zstd)

It is implemented in C library, so to make it usable in Java this project is used: [zstd-jni](https://github.com/luben/zstd-jni)

## Installation

You can build the project yourself or access it via Gradle:

`compile 'com.github.matejtymes:cassandra-zstd:0.1.0'`

Maven:

```Xml
<dependency>
    <groupId>com.github.matejtymes</groupId>
    <artifactId>cassandra-zstd</artifactId>
    <version>0.1.0</version>
</dependency>
```

or your other preferred dependency manager.

Then you have to copy the final jar and its zstd-jni dependency into the cassandra folder

```{r, engine='bash'}
cp cassandra-zstd-{version}.jar {cassandra_home}/lib
cp zstd-jni-{version}.jar {cassandra_home}/lib
```

## Configuration

To create a new table with Zstandard compression enabled you have to add this setting to it:
 
```Sql
CREATE TABLE KEYSPACE_NAME.TABLE_NAME (
  ...
) WITH compression = { 'sstable_compression': 'org.apache.cassandra.io.compress.ZstdCompressor', [options] }

```

To update the compression algorithm on already existing table please execute this command:

```Sql
ALTER TABLE KEYSPACE_NAME.TABLE_NAME 
WITH compression = { 'sstable_compression': 'org.apache.cassandra.io.compress.ZstdCompressor', [options] }
```

## Options

There is currently only one option available

- **compression_level** - if no value is defined 1 will be used as the default value. (To find more details about the compression levels please consult the *Picking a compression level* section in [here](https://code.facebook.com/posts/1658392934479273/smaller-and-faster-data-compression-with-zstandard/))
  
You can choose to omit the options and the defaults will be used:
```Sql
... WITH compression = { 'sstable_compression': 'org.apache.cassandra.io.compress.ZstdCompressor' }
```
or you can define your own value:
```Sql
... WITH compression = { 'sstable_compression': 'org.apache.cassandra.io.compress.ZstdCompressor',  'compression_level': '16'}
```
