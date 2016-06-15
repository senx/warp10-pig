REGISTER warp10-pig.jar;

REGISTER lib/elephant-bird-core-4.6.jar;
REGISTER lib/elephant-bird-hadoop-compat-4.6.jar;
REGISTER lib/elephant-bird-pig-4.6.jar;

DEFINE GTSDump io.warp10.pig.GTSDump('optimize');
DEFINE GTSWrapperLoad io.warp10.pig.GTSWrapperFromSF('normal','false');
DEFINE GTSCount io.warp10.pig.GTSCount();

DEFINE GTSUpload io.warp10.pig.GTSUpload('-t XXXX -u http://XXXX/api/v0/update -c');

gtsWrappers = load '$input' USING BinStorage() AS (encoded: bytearray);
DESCRIBE gtsWrappers;

gtsUpload = FOREACH gtsWrappers GENERATE GTSUpload(encoded);

DUMP gtsUpload;

--pointsPerGts = FOREACH gtsWrappers GENERATE FLATTEN(GTSCount(encoded)) AS (nb: long);
--DESCRIBE pointsPerGts;
