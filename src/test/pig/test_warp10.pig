REGISTER build/libs/warp10-pig-dmn.jar;
--REGISTER lib/warpscript-SNAPSHOT.jar;

REGISTER lib/elephant-bird-core-4.6.jar;
REGISTER lib/elephant-bird-hadoop-compat-4.6.jar;
REGISTER lib/elephant-bird-pig-4.6.jar;

--SET warp10.fetcher.fallbacks 'warp.cityzendata.net';
--SET warp10.fetcher.protocol 'https';
SET warp10.fetcher.fallbacks 'localhost';
SET warp10.fetcher.protocol 'http';
SET http.header.now 'X-CityzenData-Now';
SET http.header.timespan 'X-Warp10-Timespan';
--SET warp10.fetcher.port '443';
SET warp10.fetcher.port '8881';
--SET warp10.fetcher.path '/dist/api/v0/sfetch';
SET warp10.fetcher.path '/api/v0/sfetch';
--SET warp10.splits.endpoint 'https://warp.cityzendata.net/dist/api/v0/splits';
SET warp10.splits.endpoint 'http://localhost:8881/api/v0/splits';
SET warp10.fetch.timespan '$timespan';
SET warp10.http.connect.timeout '2000';
SET warp10.max.splits '200';

SET warp10.splits.token '$token';
SET warp10.splits.selector '$selector';
SET warp10.fetch.now '$now';
SET warp10.fetch.timespan '$timespan';

DEFINE GTSDump io.warp10.pig.GTSDump('optimize');
DEFINE GTSWrapperLoad io.warp10.pig.GTSWrapperFromSF('normal','false');
DEFINE GTSWrapperComputeSize io.warp10.pig.GTSWrapperSize();


data = LOAD '$selector' USING io.warp10.pig.Warp10LoadFunc();
DESCRIBE data;

gts = FOREACH data GENERATE data;
DESCRIBE gts;

gtsSize = FOREACH gts GENERATE FLATTEN(GTSWrapperComputeSize(data)) AS (dataSize: int);
DESCRIBE gtsSize;

sizeGroup = GROUP gtsSize ALL;
DESCRIBE sizeGroup;

gtsSum = FOREACH sizeGroup GENERATE SUM(gtsSize.dataSize);
DESCRIBE gtsSum;

DUMP gtsSum;

--storeData = STORE gts INTO '$output' USING io.warp10.pig.utils.SequenceFileWriter('org.apache.hadoop.io.BytesWritable','org.apache.hadoop.io.BytesWritable');

--
-- test Store
--

--raw_data = load '$output' using com.twitter.elephantbird.pig.load.SequenceFileLoader(
--'-c com.twitter.elephantbird.pig.util.BytesWritableConverter','-c com.twitter.elephantbird.pig.util.BytesWritableConverter')
-- AS (key, value);

--gtsLoad = FOREACH raw_data GENERATE FLATTEN(GTSWrapperLoad(key, value));
--DESCRIBE gtsLoad;

--gtsDump = FOREACH gtsLoad GENERATE GTSDump(encoded) AS gts;
--DUMP gtsDump;
