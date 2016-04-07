REGISTER build/libs/lepton-dmn-full.jar

REGISTER lib/elephant-bird-core-4.6.jar;
REGISTER lib/elephant-bird-hadoop-compat-4.6.jar;
REGISTER lib/elephant-bird-pig-4.6.jar;

DEFINE GTSWrapperLightLoad io.warp10.pig.UDFWrapper('GTSWrapperFromSF','light');
DEFINE GTSWrapperLoad io.warp10.pig.UDFWrapper('GTSWrapperFromSF','normal');
DEFINE GTSCount io.warp10.pig.UDFWrapper('GTSCount');

rawData = load '$input' using com.twitter.elephantbird.pig.load.SequenceFileLoader(
'-c com.twitter.elephantbird.pig.util.BytesWritableConverter','-c com.twitter.elephantbird.pig.util.BytesWritableConverter')
 AS (key, value);

--
-- Count datapoints
--

gts = FOREACH rawData GENERATE FLATTEN(GTSWrapperLoad(key,value));
DESCRIBE gts;

gtsCount = FOREACH gts GENERATE GTSCount(gts::encoded);

gtsGroup = GROUP gtsCount ALL;
DESCRIBE gtsGroup;

--gtsNb = FOREACH gtsGroup GENERATE COUNT(gtsCount);
--DESCRIBE gtsNb;

--DUMP gtsNb;

gtsSum = FOREACH gtsGroup GENERATE SUM(gtsCount.nbTicks);
DESCRIBE gtsSum;

DUMP gtsSum;




