REGISTER build/libs/lepton-full.jar

--
-- Warpscript for reduce
--
REGISTER $script

DEFINE GTSWrapperLightLoad io.warp10.pig.UDFWrapper('GTSWrapperFromSF', 'light');
DEFINE GTSWrapperChunkLoad io.warp10.pig.UDFWrapper('GTSWrapperFromSF','chunk','$clipFrom','$clipTo','$chunkwidth');
DEFINE GTSWrapperLoad io.warp10.pig.UDFWrapper('GTSWrapperFromSF','normal');
DEFINE Reduce io.warp10.pig.UDFWrapper('Reduce','$script');
DEFINE Fuse io.warp10.pig.UDFWrapper('Fuse');
DEFINE GenReduceId io.warp10.pig.UDFWrapper('GenReduceId','$labels');
DEFINE GenLabelsPart io.warp10.pig.UDFWrapper('GenLabelsPart','$labels');
DEFINE GTSDump io.warp10.pig.UDFWrapper('GTSDump');

--
--  Reduce
-- @param script : Warpscript for reduce
-- @param labels : labels on which equivalence classes will be done - (Ex : app,foo)
-- @param clipFrom : first timestamp in microseconds for chunk op - String representation of long (Ex : 10629342490L)
-- @param clipTo : last timestamp in microseconds for chunk op - String representation of long (Ex : 14893948418L)
-- @param chunkwidth : size of each chunk in microseconds for chunk op - String representation of long (Ex : 10000000L)
--


--
-- Label name for chunk part - Could be passed as param
--

%default chunkLabelname 'chunkId';

--
-- key is an instance of GTSWrapper
-- value is an instance of GeoTimeSerie
-- We can Filter on key attributes (labels, ...) here before the merge
--

rawData = load '$input' using com.twitter.elephantbird.pig.load.SequenceFileLoader(
'-c com.twitter.elephantbird.pig.util.BytesWritableConverter','-c com.twitter.elephantbird.pig.util.BytesWritableConverter')
AS (key, value);

--
-- Generate GTSWrapper without ticks to compute reduceId
--

gtsLight = FOREACH rawData GENERATE FLATTEN(GTSWrapperLightLoad(key));

DESCRIBE gtsLight;

gtsLight = FOREACH gtsLight GENERATE gts::classIdForJoin AS classIdForJoin, gts::labelsIdForJoin AS labelsIdForJoin, gts::encoded AS encoded;

DESCRIBE gtsLight;

--
-- Gen reduceId by partition
--

gtsLightGroup = GROUP gtsLight ALL;

DESCRIBE gtsLightGroup;

gtsLightPart = FOREACH gtsLightGroup GENERATE FLATTEN(GenReduceId(gtsLight));

DESCRIBE gtsLightPart;

--
-- Generate chunks (without reduceId)
--

chunkGen = FOREACH rawData GENERATE FLATTEN(GTSWrapperChunkLoad(key, value));

DESCRIBE chunkGen;

test = FOREACH chunkGen GENERATE GTSDump(encoded);
--DUMP test;

chunk = FOREACH chunkGen GENERATE gts::labels AS labels, gts::chunkId AS chunkId, gts::classIdForJoin AS classIdForJoin, gts::labelsIdForJoin AS labelsIdForJoin, gts::encoded AS encoded;

DESCRIBE chunk;

--
-- JOIN with gtsLightPart to add reduceId
--

chunkFullJoin = JOIN chunk BY (classIdForJoin, labelsIdForJoin) LEFT OUTER, gtsLightPart BY (partitions::classIdForJoin, partitions::labelsIdForJoin) USING 'replicated';

DESCRIBE chunkFullJoin;


chunkFull = FOREACH chunkFullJoin GENERATE chunk::labels AS labels, chunk::chunkId AS chunkId, gtsLightPart::partitions::reduceId AS reduceId, chunk::encoded AS encoded;

DESCRIBE chunkFull;

--
-- Generate a value against the labels we want as equiv classes (partitions). * for all partitions
--

chunkFullEquiv = FOREACH chunkFull GENERATE labels, chunkId, reduceId, encoded, GenLabelsPart(labels) AS labelsEquiv;

--
-- Ignore chunks where String that stands for equivalence classes is null
--

chunkFullEquivFiltered = FILTER chunkFullEquiv BY labelsEquiv IS NOT NULL;

--
-- Group data per equivalence class and chunkId
--

gtsGroup = GROUP chunkFullEquivFiltered BY (labelsEquiv,chunkId);

DESCRIBE gtsGroup;

--
-- Launch reduce
--

gtsReduce = FOREACH gtsGroup GENERATE FLATTEN(Reduce(chunkFullEquivFiltered));

DESCRIBE gtsReduce;

--
-- Group by reduce id
--

gtsReduceMerged = GROUP gtsReduce BY reduceId;

DESCRIBE gtsReduceMerged;

--
-- Fuse chunks
--

gtsFuse = FOREACH gtsReduceMerged GENERATE group, Fuse(gtsReduce);

DESCRIBE gtsFuse;

gtsFuseTest = GROUP gtsFuse ALL;
gtsFuseTest2 = FOREACH gtsFuseTest GENERATE COUNT(gtsFuse);
DUMP gtsFuseTest2;



