REGISTER warp10-pig.jar;

REGISTER lib/elephant-bird-core-4.6.jar;
REGISTER lib/elephant-bird-hadoop-compat-4.6.jar;
REGISTER lib/elephant-bird-pig-4.6.jar;

DEFINE GTSDump io.warp10.pig.GTSDump('optimize');
DEFINE GTSWrapperLoad io.warp10.pig.GTSWrapperFromSF('normal','true');
DEFINE GTSCount io.warp10.pig.GTSCount();

raw_data = load '$input' using com.twitter.elephantbird.pig.load.SequenceFileLoader('-c com.twitter.elephantbird.pig.util.BytesWritableConverter','-c com.twitter.elephantbird.pig.util.BytesWritableConverter') AS (key, value);

gtsLoad = FOREACH raw_data GENERATE FLATTEN(GTSWrapperLoad(key, value)) AS (className: chararray,labels: map[],classId: long,labelsId: long, encoded: bytearray);
DESCRIBE gtsLoad;

gtsWrappers = FOREACH gtsLoad GENERATE classId, labelsId, encoded;
DESCRIBE gtsWrappers;

pointsPerGts = FOREACH gtsWrappers GENERATE FLATTEN(GTSCount(encoded)) AS (nb: long);
DESCRIBE pointsPerGts;

pointsPerGtsGroup = GROUP pointsPerGts ALL;
DESCRIBE pointsPerGtsGroup;

gtsSum = FOREACH pointsPerGtsGroup GENERATE SUM(pointsPerGts.nb);
DESCRIBE gtsSum;

DUMP gtsSum;


