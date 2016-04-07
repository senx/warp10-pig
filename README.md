# Warp10-pig #

Warp10-pig is part of the Cityzen Data platform. [Apache Pig](http://pig.apache.org/) and Pig-Latin, its dataflow language, is part of the Hadoop Ecosystem. Warp10-pig empowers Pig to manipulate GTS (Geo Time Series) with Warpscript, the data manipulation language created and maintened by Cityzen Data.
With Warp10-pig you can use your traditional Pig scripts and enrich them with the use of Warpscript. Warp10-pig provides a list of Pig UDFs (User Defined Functions) to fit your data for the use of Warpscript and then, get the result from the Warpscript execution.

# Warpscript #

The main documentation about Warpscript, Geo Time Series (GTS) and all about the Cityzen Data platform can be found [here](https://api0.cityzendata.net/doc/)

# Prerequisites #

## Technicals ##

- Pig 0.14+ http://pig.apache.org/
- Hadoop 1.x / 2.x http://hadoop.apache.org/ (Hadoop is not required in Pig `local` mode)
- Memory (min): 4 GB

##  About Warp10-pig ##

Some prerequisites must be known for using Warpscript with Warp10-pig dued to the fact that Warp10-pig will be used in batch mode and in a distributed way. These prerequisites are described here after.

# GTS format #

## Purpose ##

I have a Pig script with data that represents Geo Time Series and I want to launch an Warpscript. 

- How can I push my data into Warp10-pig ?
- How can I launch my Warpscript ?
- How can I retrieve the data after the Warpscript execution ?

## Howto ##

### GTSWrapper ###

GTSWrapper is the thrift representation of one Geo Time Serie. Each GTSWrapper instance contains a byte array that represents the Geo Time Serie values encoded in order to be serializable and some another informations:

- Metadata for the GTS: class name, labels: [], attributes: []
- Last bucket for bucketized GTS
- Bucket span for bucketized GTS
- Bucket count for bucketized GTS
- Base timestamp, if not set assumed to be 0
- Number of datapoints encoded in the wrapped GTS
- Encoded GTS content (datapoints): this is the Geo Time Serie values.

GTSWapper is packed as DataByteArray (bytearray) in Pig. Thus, a bytearray in Pig with Warp10-pig stands for a packed GTSWapper.

### Input ###

We choose a convention to parse GTS with Warp10-pig. You have to provide your data in a specific way with Pig.
It's quite simple: each value of your GTS (timestamp, latitude, longitude, elevation, value) is a field of one Tuple (Pig). And this Tuple is wrapped in a Bag.

Here after the different Pig formats about one GTS that Warp10-pig is able to parse as input:

~~~~
- {(ts, lat, lon, elev, value)} or {metadata: [], {(ts, lat, lon, elev, value)}}
- {(ts, lat, lon, value)} or {metadata: [], {(ts, lat, lon, value)}}
- {(ts, elev, value)} or {metadata: [], {(ts, elev, value)}}
- {(ts, value)} or {metadata: [], {(ts, value)}}

where:
- ts: timestamp (long)
- lat: latitude (double)
- lon: longitude (double)
- elev: elevation (double)
- value: depends of your data..
- metadata: [class#XX,labels#{key=value},attributes#{key=value}] 
~~~~

**Important**: all data in one Bag concern the same GTS

### Output ###

The different output formats concerning one GTS provided by Warp10-pig are quite the same. We just add one field: metadata (Map).

~~~~
- (metadata: [], {(ts, lat, lon, elev, value)})
- (metadata: [], {(ts, lat, lon, value)})
- (metadata: [], {(ts, elev, value)})
- (metadata: [], {(ts, value)})
~~~~

### Warpscript stack ###

Warp10-pig represents the Warpscript stack with the concept of Bag in Pig. The default cast operators used by Pig as shown [here](http://pig.apache.org/docs/r0.14.0/basic.html#cast) are used except of bytearray. Bytearray has no representation in the stack. Thus, bytearray value stands for a GTSwrapper packed as DataByteArray.

### Warp10-pig macros ###

Warp10-pig is composed of [UDFs](#UDFs) in Java and Pig Macros. These macros are defined in `Warp10-pig.pig`. This file will have to be registered in every Pig scripts. These macros are described more precisely in `Warp10-pig.pig`.

Current macros are these ones:

`Warp10-pigOverlap`: Warp10-pigOverlap will transform the input Geo Time Series by attempting to add a prefix and suffix to each of them so two consecutive GTS overlap by a specified amount.
 
~~~~
 /**
 * @param timespan Duration of prefix and suffix to add to each input Geo Time Series
 * @param GTS Relation with Warp10-pig Geo Time Series
 *
 * @return a relation with overlapping Geo Time Series
 */
~~~~

`GetObjectFromStackLevel`: Extract an object (element) from one level of the stack.

~~~~
/**
* @param One stack level that contains an element: (scriptId: long,inputId: long,level: int,value: {(obj: XX)})
*
* @return (obj: XX)
*/
~~~~

`GetObjectFromStack`: Extract an object (element) from the stack at one level.

~~~~
/**
* @param level of the stack at which the object is located
* @param the full stack: { (scriptId: long,inputId: long,level: int,value: {(obj: XX)}) }
*
* @return (obj: XX)
*/
~~~~

***

# Example #

Here after, how to launch Warp10-pig through a brief example.

## Macros ##

[`Warp10-pig.pig`](#Warp10-pig macros) contains a list of Pig macros. This file is provided with Warp10-pig. 

Macros required for this test are:

~~~~
/**
* Extract an object (element) from one level of the stack.
*
* @param One stack level that contains an element: { (scriptId: long,inputId: long,level: int,value: {(obj: XX)})
*
* @return (obj: XX)
*
*/
DEFINE GetObjectFromStackLevel(STACKLEVEL) RETURNS OBJ {

FLAT1 = FOREACH $STACKLEVEL GENERATE FLATTEN($0);

FLAT2 = FOREACH FLAT1 GENERATE FLATTEN($3);

$OBJ = FOREACH FLAT2 GENERATE FLATTEN($0) AS obj;

};

/**
* Extract an object (element) from the stack at one level.
*
* @param level of the stack at which the object is located
* @param the full stack: { (scriptId: long,inputId: long,level: int,value: {(obj: XX)}) }
*
* @return (obj: XX)
*
*/
DEFINE GetObjectFromStack(INDEX, STACK) RETURNS OBJ {

STACKLEVEL = FOREACH $STACK GENERATE GetFromStack($INDEX, $0);

$OBJ = GetObjectFromStackLevel(STACKLEVEL);

};
~~~~

## Warpscript ##

`test.mc2`:

~~~~
//
// test: value = value + 2
//
2.0 mapper.add
0 0 0
5 ->LIST MAP
~~~~

## Data ##

`test.csv`. Row format is: timestamp,latitude,longitude,value

~~~~
1382441207762000,51.501988,0.005953,79.16
1382441237727000,51.501988,0.005953,75.87
1382441267504000,51.501988,0.005953,74.46
1382441267504000,51.501988,0.005953,73.55
1382441297664000,51.501988,0.005953,72.30
1382441327765000,51.501988,0.005953,70.73
1382441327765000,51.501988,0.005953,69.50
1382441357724000,51.501988,0.005953,68.24
1382441387792000,51.501988,0.005953,66.66
1382441387792000,51.501988,0.005953,65.73
~~~~

## Pig ##

`test.pig`:

```Pig
--
-- Test: Load GeoTime Series from CSV file and launch Warpscript operations onto these ones
--

REGISTER Warp10-pig-shielded.jar;
REGISTER $mc2;

IMPORT '$Warp10-pig_macros';

DEFINE WarpscriptRun UDFWrapper('WarpscriptRun');
DEFINE ConvertToWarp10-pig UDFWrapper('ConvertToWarp10-pig');
DEFINE ConvertFromWarp10-pig UDFWrapper('ConvertFromWarp10-pig');
DEFINE GetFromStack UDFWrapper('GetFromStack');
DEFINE GTSDump UDFWrapper('GTSDump');

--
-- Load CSV file with GeoTime Series
-- Expected formats:
-- 'CSV like': ts, lat, lon, elev, value with one line per value OR 'Pig like': {(ts, lat, lon, elev, value)} or {metadata: [], {(ts, lat, lon, elev, value)}} 
-- 'CSV like': ts, lat, lon, value with one line per value OR 'Pig like': {(ts, lat, lon, value)} or {metadata: [], {(ts, lat, lon, value)}}
-- 'CSV like': ts, elev, value with one line per value OR 'Pig like': {(ts, elev, value)} or {metadata: [], {(ts, elev, value)}}
-- 'CSV like': ts, value with one line per value OR 'Pig like': {(ts, value)} or {metadata: [], {(ts, value)}}
--

data = load '$input' USING PigStorage(',') AS (ts:long,lat:double,long:double,value:double);

dataGroup = GROUP data ALL;

--
-- Convert these TimeSeries data from Pig to GTS (import)
--

gtsWrappers = FOREACH dataGroup GENERATE ConvertToWarp10-pig(data);

--
-- Launch Warpscript onto one Bag of data
--

gtsExec = FOREACH gtsWrappers GENERATE WarpscriptRun('@$mc2',gts) AS stack;

--
-- We expect one GTS (instance of GTSWrapper packed as DataByteArray) in result of the previous WarpscriptRun on top of the stack. So Get it !
--

gtsExtract = GetObjectFromStack(0, gtsExec);

--
-- Convert GTSWrapper to Pig (export)
--

gtsToPig = FOREACH gtsExtract GENERATE ConvertFromWarp10-pig(obj);

DUMP gtsToPig

--
-- Or Dump this GTSWrapper (Json like)
--

-- gtsDump = FOREACH gtsExtract GENERATE GTSDump(obj);

-- DUMP gtsDump;
```

## Usage ##

~~~~
pig [-x local] -p input='test.csv' -p Warp10-pig_macros='Warp10-pig.pig' -p mc2='test.mc2' -f src/test/pig/test.pig
~~~~

## Result ##

You should expect this result: 

~~~~
(([bucketspan#0,bucketcount#0,count#0,attributes#{},class#,lastbucket#0,base#0,labels#{}],{(1382441207762000,51.50198796764016,0.005952995270490646,81.16),(1382441237727000,51.50198796764016,0.005952995270490646,77.87),(1382441267504000,51.50198796764016,0.005952995270490646,75.55),(1382441267504000,51.50198796764016,0.005952995270490646,75.55),(1382441297664000,51.50198796764016,0.005952995270490646,74.3),(1382441327765000,51.50198796764016,0.005952995270490646,71.5),(1382441327765000,51.50198796764016,0.005952995270490646,71.5),(1382441357724000,51.50198796764016,0.005952995270490646,70.24),(1382441387792000,51.50198796764016,0.005952995270490646,67.73),(1382441387792000,51.50198796764016,0.005952995270490646,67.73)}))
~~~~

***

# UDFs #

Here after, you'll find the list of the main Pig UDFs provided by Warp10-pig to deal with your GTS and to execute Warpscript code.

## ConvertToWarp10-pig ##

### description ###

This is the main UDF to inject your data into Warp10-pig. It stands for the bridge between your data (in Pig) and the Cityzen Data's platform.
This UDF converts a bag of tuples into a bag of GTSWrappers.

### input ###

List of formats for input data (bag of tuples):

~~~~
- {(ts, lat, lon, elev, value)} or {metadata: [], {(ts, lat, lon, elev, value)}}
- {(ts, lat, lon, value)} or {metadata: [], {(ts, lat, lon, value)}}
- {(ts, elev, value)} or {metadata: [], {(ts, elev, value)}}
- {(ts, value)} or {metadata: [], {(ts, value)}}
~~~~

### output ###

Bag of GTSWrappers:

- {(encoded: bytearray)}

***

## ConvertFromWarp10-pig ##

### description ###

This UDF converts a GTS content (wrapper packed as DataByteArray) to a Pig tuple containing a map of metadata and a bag of tuples.
If the UDF is initialized with 'true' then null will be substituted for missing lat/lon/elev so the output tuples will all contain 5 elements.

### input ###

An instance of GTSWrapper (encoded):

- (encoded: bytearray)

### output ###

~~~~
- (ts, lat, lon, elev, value)
- (ts, lat, lon, value)
- (ts, elev, value)
- (ts, value)
~~~~

depending on the presence or not of lat/lon, elev and if the nullify mode is set.

***

## WarpscriptRun ##

### description ###

Exec an Warpscript file or Warpscript commands onto a Stack that contains a Bag of data (GTS).
Warpscript should start with `@`

### input ###

A tuple with 2 fields: 

(mc2: string, {(encoded: bytearray)})
with mc2: '@mc2' or 'NOW..'

### output ###

The Warpscript stack after exec:

stack: {(scriptId: long,inputId: long,level: int,value: {()})}

- scriptId: hash of the Warpscript (or Eisntein commands)
- inputId: hash of data
- level: current level
- value: the current object at this level of the stack

The pair (scriptId, inputId) identifies the current execution. The same script applied on the same data produces the same value (scriptId, inputId). With these uuids 

***

## FilterByMetadata ##

### description ###

Filter a Bag of GTSWrappers onto the class name parameter.  
This UDF is created with the name of the filter parameter and the value.

### input ###

Once the UDF has been init with the name of the filter parameter and its value, this UDF only requires data we want filter, a bag of GTSWrappers.

- {(encoded: bytearray)}

### output ###

A bag of GTSwrappers that match this filter:

- {(encoded: bytearray)}

***

## Fuse ##

### description ###

Fuse a list of GTS.

### input ###

A bag of GTSWrappers we want fuse:

- {(encoded: bytearray)}

### output ###

A bag that contains the GTSWrapper just 'fused'

- {(encoded: bytearray)}

***

## GTSCount ##

### description ###

Returns the number of datapoints in GTS. Returns the Count parameter of the GTSWrapper. If not set, returns the number of ticks in the Geo Time Serie wrapped by this GTSWrapper.

### input ###

An instance of GTSwrapper:

- (encoded: bytearray)

### output ###

- (nbTicks: int)

***

## GTSDump ##

### description ###

Dump a GTS. Input Format is used to display GTS as described [here](https://api0.cityzendata.net/doc/api/gts-input-format)

### input ###

An instance of GTSwrapper:

- (encoded: bytearray)

### output ###

A bag with one tuple that contains the String representation of this GTS:

- {(TS/LAT:LON/ELEV NAME{LABELS} VALUE)}

***

## GTSWrapperFromSF ##

### description ###

This UDF merges the key,value of one SequenceFile in one instance of GTSWrapper.

**Prerequisite**: SequenceFile must contain k,v records with 

- key: BytesWritable - key is an instance of GTSWrapper packed as DataByteArray where datapoints should not be included.
- value: BytesWritable - value only contains datapoints of the GTS.

To load the SequenceFile, you can use loader like the one provided by elephant-bird: 

`rawData = LOAD 'XXX' using com.twitter.elephantbird.pig.load.SequenceFileLoader(
'-c com.twitter.elephantbird.pig.util.BytesWritableConverter','-c com.twitter.elephantbird.pig.util.BytesWritableConverter')
AS (key, value);`

**Modes**: the 'mode' parameter must be provided at init. 3 modes:

- `normal`: key, value are read in a classical way.
- `light`: only the key (GTSWrapper) is read. Thus, if the GTSWrapper does not contain datapoints, this is quite more efficient. This is useful only if we have to use metadata of the GTS without datapoints.
- `chunk`: this mode is specific to GTS that contains too many datapoints and must ne split in 'chunks'.

### input ###

- (key: bytearray, value: bytearray)

### output ###

A bag of GTSWrappers ;

- {(encoded: bytearray)}

***

## GTSWrapperToSF ##

### description ###

This is the opposite of GTSWrapperFromSF. This UDF converts a tuple of GTSWrapper to a tuple of key,value with:

- key: BytesWritable - Instance of GTSWrapper packed as DataByteArray where datapoints should not be included.
- value: BytesWritable - datapoints of the GTS.

### input ###

- (encoded: bytearray)

### output ###

- (key: bytearray, value: bytearray)

***

## GenLabelsPart ##

### description ###

This UDF returns a value against the list of labels we want as equivalence classes (Partition). 
The list of labels have to be provided during instanciation.

### input ###

A tuple that contains a map with all labels:

- ([key#value,...])

### output ###

The concatenation of labels ('labelKey1#value,labelKey2#value,...') we want as equivalence class:

- (labelsEquiv: chararray)

***

## GenReduceId ##

### description ###

This UDF returns the partitions and gen the reduce id against the common labels of GTS. We have one partition per common label and thus one reduce id.

**Important**: datapoints of these GTSWrappers are not necessary here. On the contrary, values should be omitted because this UDF have to parse all GTS to compute the reduceId. 

### input ###

Bag of GTSWrappers with their external ids (classIdForJoin and labelsIdForJoin):

- {(classIdForJoin: long, labelsIdForJoin: long, encoded: bytearray)}

### output ###

- {(classIdForJoin: long, labelsIdForJoin: long, reduceId: chararray)}

***

## GetFromStack ##

### description ###

Get object from the Warpscript stack at one position.

### input ###

A tuple with the level of the stack and the current stack (Bag).

- (level: int, stack: {})

### output ###

- (scriptId: long,inputId: long,level: int,value: {()})

***

## TicksBoundaryFromSF ##

### description ###

Returns a tuple with the first and the max timestamp of one GTSWrapper. Input is the key,value of one Sequence File with:

- key: BytesWritable - Instance of GTSWrapper packed as DataByteArray where datapoints should not be included.
- value: BytesWritable - datapoints of the GTS.

### input ###

- (key: bytearray, value: bytearray)

### output ###

- (firstTick: long, lastTick: long)

***

## UnpackGTSMetadata ##

### description ###

Output a map with Metadata from a GTSWrapper.

### input ###

- (encoded: bytearray)

### output ###

- ([class#XX,lastbucket#XX,base#XX,labels#{labelKey1=XX}])

***

## UUID ##

### description ###

Use Warpscript to generate an UUID.

### input ###

- none

### output ###

- (uuid: chararray)