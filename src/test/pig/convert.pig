REGISTER build/libs/warp10-pig-0.0.1-3-gf2bb447.jar;

REGISTER lib/elephant-bird-core-4.6.jar;
REGISTER lib/elephant-bird-hadoop-compat-4.6.jar;
REGISTER lib/elephant-bird-pig-4.6.jar;

DEFINE GTSDump io.warp10.pig.GTSDump('optimize');
DEFINE GTSWrapperLoad io.warp10.pig.GTSWrapperFromSF('normal','false');
DEFINE GTSCount io.warp10.pig.GTSCount();
DEFINE WarpScriptRun io.warp10.pig.WarpScriptRun();

DEFINE InputFormatToGTS io.warp10.pig.InputFormatToGTS();

DEFINE GTSUpload io.warp10.pig.GTSUpload('-t XXXX -u http://XXXX/api/v0/update -c');

gtsStr = LOAD '$input' USING PigStorage() AS (data: chararray);

--
-- Convert into GTS
--

gts = FOREACH gtsStr GENERATE InputFormatToGTS(data) AS encoded;

--
-- GENERATE ((selector,encoded))
--

gtsUnWrap = FOREACH gts GENERATE WarpScriptRun('] UNWRAP LIST-> DROP DUP WRAPRAW SWAP TOSELECTOR', encoded) AS stack;

--
-- ((selector,encoded))
-- Group By selector
--

bySelector = GROUP gtsUnWrap BY $0.$0;

--
-- Merge GTS (merge values related to the same selector)
--

merge = FOREACH bySelector GENERATE FLATTEN(WarpScriptRun('] <% DROP 0 REMOVE DROP %> LMAP FLATTEN UNWRAP MERGE', $1)) AS encoded: bytearray;


display = FOREACH merge GENERATE GTSDump(encoded);
DUMP display;
