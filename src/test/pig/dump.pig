REGISTER build/libs/warp10-pig-0.0.9-rc2.jar;

REGISTER lib/elephant-bird-core-4.6.jar;
REGISTER lib/elephant-bird-hadoop-compat-4.6.jar;
REGISTER lib/elephant-bird-pig-4.6.jar;

REGISTER warp10.conf;
SET warp10.config 'warp10.conf';

SET warp10.fetcher.fallbacks 'XXXX,XXXX';
SET warp10.fetcher.protocol 'http';
SET http.header.now 'X-CityzenData-Now';
SET http.header.timespan 'X-Warp10-Timespan';
SET warp10.fetcher.port '8881';
SET warp10.fetcher.path '/api/v0/sfetch';
SET warp10.splits.endpoint 'https://XXXXX/api/v0/splits';
SET warp10.fetch.timespan '$timespan';
SET warp10.http.connect.timeout '60000';
SET warp10.http.read.timeout '60000';
SET warp10.max.splits '10';
SET pig.splitCombination false;

SET warp10.splits.token '$token';
SET warp10.splits.selector '$selector';
SET warp10.fetch.now '$now';
SET warp10.fetch.timespan '$timespan';

DEFINE GTSDump io.warp10.pig.GTSDump('optimize');

gts = LOAD '$selector' USING io.warp10.pig.Warp10LoadFunc();
DESCRIBE gts;

gtsEncoded = FOREACH gts GENERATE data;
DESCRIBE gtsEncoded;

storeData = STORE gtsEncoded INTO '$output' USING io.warp10.pig.utils.SequenceFileWriter('org.apache.hadoop.io.BytesWritable');
