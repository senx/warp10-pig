REGISTER build/libs/lepton-full.jar

--
-- Warpscript for reduce
--
REGISTER $script

DEFINE GTSWrapperLightLoad io.warp10.pig.UDFWrapper('GTSWrapperFromSF','light');
DEFINE GTSWrapperChunkLoad io.warp10.pig.UDFWrapper('GTSWrapperFromSF','chunk','$clipFrom','$clipTo','$chunkwidth');
DEFINE GTSWrapperLoad io.warp10.pig.UDFWrapper('GTSWrapperFromSF','normal');
DEFINE MapUdf io.warp10.pig.UDFWrapper('io.warp10.pig.Map','$script');
DEFINE Fuse io.warp10.pig.UDFWrapper('Fuse');
DEFINE GTSDump io.warp10.pig.UDFWrapper('GTSDump');

--
--  Map
-- @param script : Warpscript for map
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
-- Generate chunks
--

chunkGen = FOREACH rawData GENERATE FLATTEN(GTSWrapperChunkLoad(key, value));

DESCRIBE chunkGen;

chunk = FOREACH chunkGen GENERATE gts::labels AS labels, gts::gtsId AS gtsId, gts::chunkId AS chunkId, gts::encoded AS encoded;

DESCRIBE chunk;

--
-- Launch map
--

gtsMap = FOREACH chunk GENERATE gtsId, MapUdf(encoded);

DESCRIBE gtsMap;

--
-- Group by gtsId
--

gtsMapGroup = GROUP gtsMap BY gtsId;

DESCRIBE gtsMapGroup;

gtsMapGroupTest = FOREACH gtsMapGroup GENERATE group, FLATTEN(gtsMap);
DESCRIBE gtsMapGroupTest;
gtsMapGroupTest2 = FOREACH gtsMapGroupTest GENERATE GTSDump(encoded);
DUMP gtsMapGroupTest2;

