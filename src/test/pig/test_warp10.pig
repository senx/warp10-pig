--
-- Test: Load GeoTime Series from CSV file and launch Warpscript operations onto these ones
-- Usage: pig [-x local] -p input='test.csv' -p lepton_macros='lepton.pig' -p mc2='test.mc2' -f src/test/pig/test.pig
--

REGISTER build/libs/warp10-pig.jar;
--REGISTER build/libs/warpscript-SNAPSHOT.jar;

SET warp10.fetcher.fallbacks 'warp.cityzendata.net';
SET warp10.fetcher.protocol 'https';
SET http.header.now 'X-CityzenData-Now';
SET http.header.timespan 'X-Warp10-Timespan';
SET warp10.fetcher.port '443';
SET warp10.fetcher.path '/dist/api/v0/sfetch';
SET warp10.splits.endpoint 'https://warp.cityzendata.net/dist/api/v0/splits';
SET warp10.fetch.timespan '$timespan';
SET warp10.http.connect.timeout '2000';

SET warp10.splits.token '$token';
SET warp10.splits.selector '$selector';
SET warp10.fetch.now '$now';
SET warp10.fetch.timespan '$timespan';

DEFINE GTSDump io.warp10.pig.GTSDump('optimize');
DEFINE ConvertFromLepton io.warp10.pig.ConvertFromLepton();


data = LOAD '$selector' USING io.warp10.pig.Warp10LoadFunc();

DESCRIBE data;

gtsDump = FOREACH data GENERATE GTSDump(data) AS gts;

DUMP gtsDump;