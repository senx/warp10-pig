REGISTER build/libs/lepton-dmn-full.jar

REGISTER lib/elephant-bird-core-4.6.jar;
REGISTER lib/elephant-bird-hadoop-compat-4.6.jar;
REGISTER lib/elephant-bird-pig-4.6.jar;

DEFINE GTSWrapperLightLoad io.warp10.pig.UDFWrapper('GTSWrapperFromSF','light');
DEFINE GTSWrapperLoad io.warp10.pig.UDFWrapper('GTSWrapperFromSF','normal');
DEFINE WarpscriptStackUdf io.warp10.pig.UDFWrapper('Reduce','null');
DEFINE WarpscriptStackFuse io.warp10.pig.UDFWrapper('Fuse');
DEFINE TicksBoundaryFromSF io.warp10.pig.UDFWrapper('TicksBoundaryFromSF');
DEFINE GTSDump io.warp10.pig.UDFWrapper('GTSDump','optimize');
DEFINE GTSUpload UDFWrapper('GTSUpload','-t GSMA.MAE.WRITE -u http://5.39.68.206:8080/api/v0/update -c');

raw_data = load '$input' using com.twitter.elephantbird.pig.load.SequenceFileLoader(
'-c com.twitter.elephantbird.pig.util.BytesWritableConverter','-c com.twitter.elephantbird.pig.util.BytesWritableConverter')
 AS (key, value);

gtsLoad = FOREACH raw_data GENERATE FLATTEN(GTSWrapperLoad(key, value));

gtsLoad = LIMIT gtsLoad 5;

--gtsDump = FOREACH gtsLoad GENERATE gts::labels AS labels, GTSDump(gts::encoded) AS gts;

--gtsDump = LIMIT gtsDump 10;

--DUMP gtsDump;

gtsUpload = FOREACH gtsLoad GENERATE GTSUpload(gts::encoded);
DESCRIBE gtsUpload;

DUMP gtsUpload;

--
-- Display first and last tick
-- FIXME : Use GeotimeSerie
--

--gtsTick = FOREACH raw_data GENERATE FLATTEN(TicksBoundaryFromSF(key, value));

--DESCRIBE gtsTick;

--gtsTick = LIMIT gtsTick 100;

--DUMP gtsTick;


--gtsGroup = GROUP gtsTick ALL;

--DESCRIBE gtsGroup;

--gtsMin = FOREACH gtsGroup {
--    gtsOrder = ORDER gtsTick BY firstTick ASC;
--    min = LIMIT gtsOrder 1;
--    GENERATE min.firstTick;
--};

--DESCRIBE gtsMin;

--DUMP gtsMin;

--gtsMax = FOREACH gtsGroup {
--   gtsOrder = ORDER gtsTick BY lastTick DESC;
--    max = LIMIT gtsOrder 1;
--    GENERATE max.lastTick;
--};

--DESCRIBE gtsMax;

--DUMP gtsMax;

--
-- Get labels
--

--gtsLight = FOREACH raw_data GENERATE FLATTEN(GTSWrapperLightLoad(key));

--gtsLabels = FOREACH gtsLight GENERATE labels;

--DESCRIBE gtsLabels;

--
-- List distinct label name
--

--gtsLabelNames = FOREACH gtsLabels GENERATE FLATTEN(KEYSET(labels)) AS labelName;

--DESCRIBE gtsLabelNames;

--gtsLabelNames2 = GROUP gtsLabelNames BY labelName;

--gtsLabelNames3 = FOREACH gtsLabelNames2 GENERATE group, COUNT(gtsLabelNames);

--DESCRIBE gtsLabelNames3;

--DUMP gtsLabelNames3;


--
-- Count by equivalence class
--

--gtsLabelGroup = GROUP gtsLabels BY (labels#'app');

--gtsLabelsCount = FOREACH gtsLabelGroup GENERATE group, COUNT_STAR(gtsLabels);

--gtsLabelsCount2 = RANK gtsLabelsCount;

--DUMP gtsLabelsCount2


--
-- Dump
--

--
-- Generate chunks (without reduceId)
--

-- gtsLoad = FOREACH raw_data GENERATE FLATTEN(GTSWrapperLoad(key, value));

--DESCRIBE gtsLoad;

-- gtsDump = FOREACH gtsLoad GENERATE gts::labels AS labels, GTSDump(gts::encoded) AS gts;

--DESCRIBE gtsDump;