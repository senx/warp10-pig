REGISTER build/libs/lepton-dmn-full.jar

REGISTER lib/elephant-bird-core-4.6.jar;
REGISTER lib/elephant-bird-hadoop-compat-4.6.jar;
REGISTER lib/elephant-bird-pig-4.6.jar;

-- REGISTER '$mc2';

DEFINE WarpscriptRun io.warp10.pig.UDFWrapper('WarpscriptRun');
DEFINE PigToGTS UDFWrapper('io.warp10.pig.PigtoGTS');
DEFINE GTSToPig io.warp10.pig.UDFWrapper('io.warp10.pig.GTStoPig');
DEFINE GetFromStack io.warp10.pig.UDFWrapper('GetFromStack');
--DEFINE GTSUpload UDFWrapper('GTSUpload','-t Hz31vXtodGlKfsnsgSLygF8N0PDpL4e.uVzt_4O2J6yVHJLQNTuH9UkNQUFSYCAt2ICzdrSoVnOnat1XCGFnADVrMDbTjQTcwU_Qs96l3IN -u https://warp.cityzendata.net/dist/api/v0/update');
DEFINE GTSUpload io.warp10.pig.UDFWrapper('GTSUpload','-t XXXX -u http://127.0.0.1:8881/dist/api/v0/update');


DEFINE GTSWrapperLoad io.warp10.pig.UDFWrapper('GTSWrapperFromSF','normal');


rawData = load '$input' using com.twitter.elephantbird.pig.load.SequenceFileLoader(
'-c com.twitter.elephantbird.pig.util.BytesWritableConverter','-c com.twitter.elephantbird.pig.util.BytesWritableConverter')
AS (key, value);

DESCRIBE rawData;

--
-- Push data
--

gtsArbs = FOREACH rawData GENERATE FLATTEN(GTSWrapperLoad(key,value));
DESCRIBE gtsArbs;

gtsArbsEncoded = FOREACH gtsArbs GENERATE encoded;
DESCRIBE gtsArbsEncoded;

gtsCount = FOREACH gtsArbsEncoded GENERATE GTSUpload(encoded);

DUMP gtsCount;

-- STORE gtsCount INTO '/mnt/czd/workspace/lepton-dmn/push_count.log';