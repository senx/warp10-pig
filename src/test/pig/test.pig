--
-- Test: Load GeoTime Series from CSV file and launch Warpscript operations onto these ones
-- Usage: pig [-x local] -p input='test.csv' -p lepton_macros='lepton.pig' -p mc2='test.mc2' -f src/test/pig/test.pig
--

REGISTER build/libs/lepton-dmn-full.jar;
REGISTER $mc2;

IMPORT '$lepton_macros';

DEFINE WarpscriptRun io.warp10.pig.UDFWrapper('WarpscriptRun');
DEFINE ConvertToLepton io.warp10.pig.UDFWrapper('ConvertToLepton');
DEFINE ConvertFromLepton io.warp10.pig.UDFWrapper('ConvertFromLepton');
DEFINE GetFromStack io.warp10.pig.UDFWrapper('GetFromStack');
DEFINE GTSDump io.warp10.pig.UDFWrapper('GTSDump');

--
-- Load CSV file with GeoTime Series
-- Expected formats:
-- 'CSV like': ts, lat, lon, elev, value with one line per value OR 'Pig like': {(ts, lat, lon, elev, value)} or {metadata: [], {(ts, lat, lon, elev, value)}} 
-- 'CSV like': ts, lat, lon, value with one line per value OR 'Pig like': {(ts, lat, lon, value)} or {metadata: [], {(ts, lat, lon, value)}}
-- 'CSV like': ts, elev, value with one line per value OR 'Pig like': {(ts, elev, value)} or {metadata: [], {(ts, elev, value)}}
-- 'CSV like': ts, value with one line per value OR 'Pig like': {(ts, value)} or {metadata: [], {(ts, value)}}
--

--
-- First mode: Pig like with one line for all all values
--
--data = LOAD '$input' USING PigStorage() AS (in:bag{t:tuple(values:bag{tval:tuple(ts:long,lat:double,lon:double,value:double)})});

--
-- CSV mode: with one line per value
--
data = LOAD '$input' USING PigStorage(',') AS (ts:long,lat:double,lon:double,value:double);


DESCRIBE data;

--
-- If one line per value (CSV like) group all these values. These values must be related to the same GTSs
--
dataGroup = GROUP data ALL;
DESCRIBE dataGroup;


--
-- Convert these TimeSeries data from Pig to GTS (import)
--

--
-- Pig mode
--
--gtsWrappers = FOREACH data GENERATE ConvertToLepton(in);

--
-- CSV mode
--
gtsWrappers = FOREACH dataGroup GENERATE ConvertToLepton(data);



DESCRIBE gtsWrappers;

--
-- Launch Warpscript onto one Bag of data
--

gtsExec = FOREACH gtsWrappers GENERATE WarpscriptRun('@$mc2',gts) AS stack;

DESCRIBE gtsExec;

--
-- We expect one GTS (instance of GTSWrapper encoded) as result of the previous WarpscriptRun on top of the stack. So Get it !
--

gtsExtract = GetObjectFromStack(0, gtsExec);

DESCRIBE gtsExtract;

--
-- Convert GTSWrapper to Pig (export)
--

gtsToPig = FOREACH gtsExtract GENERATE ConvertFromLepton(obj);

DESCRIBE gtsToPig;

DUMP gtsToPig;

--
-- Or Dump this GTSWrapper (Json like)
--

--gtsDump = FOREACH gtsExtract GENERATE GTSDump(obj);

--DUMP gtsDump;

--dataReuse = FOREACH gtsToPig GENERATE ConvertToLepton($0);
--DESCRIBE dataReuse;

--gtsDisplayReuse = FOREACH dataReuse GENERATE FLATTEN($0);
--DESCRIBE gtsDisplayReuse;

--gtsDisplayReuse2 = FOREACH gtsDisplayReuse GENERATE ConvertFromLepton($0);
--DESCRIBE gtsDisplayReuse2;
--gtsDisplayReuse3 = FOREACH gtsDisplayReuse GENERATE GTSDump($0);
--DESCRIBE gtsDisplayReuse3;

--DUMP gtsDisplayReuse3;