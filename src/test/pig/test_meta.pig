--
-- Test: Load GeoTime Series from CSV file and launch Warpscript operations onto these ones
-- Usage: pig [-x local] -p input='test.csv' -p lepton_macros='lepton.pig' -p mc2='test.mc2' -f src/test/pig/test.pig
--

REGISTER build/libs/lepton-shielded.jar;
REGISTER $mc2;

IMPORT '$lepton_macros';

DEFINE WarpscriptRun io.warp10.pig.UDFWrapper('WarpscriptRun');
DEFINE ConvertToLepton io.warp10.pig.UDFWrapper('ConvertToLepton');
DEFINE ConvertFromLepton io.warp10.pig.UDFWrapper('ConvertFromLepton');
DEFINE GetFromStack io.warp10.pig.UDFWrapper('GetFromStack');
DEFINE GTSDump io.warp10.pig.UDFWrapper('GTSDump','INTERMEDIATE');

--
-- Load CSV file with GeoTime Series
-- Expected formats:
-- {(ts, lat, lon, elev, value)}
-- {(ts, lat, lon, value)}
-- {(ts, elev, value)}
-- {(ts, value)}
--

data = LOAD '$input' USING PigStorage(',') AS (ts:long,lat:double,long:double,value:double);

DESCRIBE data;

dataGroup = GROUP data ALL;

DESCRIBE dataGroup;

--
-- Convert these TimeSeries data from Pig to GTS (import)
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

DUMP gtsToPig

--
-- Or Dump this GTSWrapper (Json like)
--

-- gtsDump = FOREACH gtsExtract GENERATE GTSDump(obj);

-- DUMP gtsDump;
