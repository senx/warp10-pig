--
-- Library of Lepton macros
--

DEFINE Fuse io.warp10.pig.UDFWrapper('Fuse');
DEFINE GTSTips io.warp10.pig.UDFWrapper('GTSTips');

/**
 * LeptonOverlap will transform the input Geo Time Series by
 * attempting to add a prefix and suffix to each of them so two
 * consecutive GTS overlap by a specified amount.
 * 
 * The first step is to extract prefixes and suffixes of duration
 * 'timespan' from each input GTS.
 * Those 'heads' and 'tails' are then associated with the GTS they
 * immediately lead or follow.
 * Each original GTS and its associated head and tail are then merged.
 *
 * @param timespan Duration of prefix and suffix to add to each input Geo Time Series
 * @param GTS Relation with Lepton Geo Time Series
 *
 * @return a relation with overlapping Geo Time Series
 */
DEFINE LeptonOverlap(timespan, IN) RETURNS GTS {
  --
  -- Generate tails and heads
  --
  TAILS_AND_HEADS = FOREACH $IN GENERATE FLATTEN(GTSTips($timespan,$0));

  --
  -- Split tails, heads and GTS
  --
  SPLIT TAILS_AND_HEADS INTO HEADS_SPLIT IF $0 == 1, TAILS_SPLIT IF $0 == 2, GTS_SPLIT IF $0 == 0;

  --
  -- Clean heads/tails/GTS
  --
  HEADS = FOREACH HEADS_SPLIT GENERATE $1 AS classId, $2 AS labelsId, $3 AS cell, $4 AS gts;
  TAILS = FOREACH TAILS_SPLIT GENERATE $1 AS classId, $2 AS labelsId, $3 AS cell, $4 AS gts;
  GTS = FOREACH GTS_SPLIT GENERATE $1 AS classId, $2 AS labelsId, $3 AS prefixCell1, $4 AS prefixCell2, $5 AS suffixCell1, $6 AS suffixCell2, $7 AS gts;

  --
  -- Join heads with GTS
  --

  SUFFIXED_1 = JOIN GTS BY (classId, labelsId, suffixCell1) LEFT OUTER, HEADS BY (classId, labelsId, cell);
  SUFFIXED = JOIN SUFFIXED_1 BY (GTS::classId, GTS::labelsId, suffixCell2) LEFT OUTER, HEADS BY (classId, labelsId, cell);

  --
  -- Join tails with GTS
  --
  PREFIXED_1 = JOIN SUFFIXED BY (GTS::classId, GTS::labelsId, prefixCell1) LEFT OUTER, TAILS BY (classId, labelsId, cell);
  PREFIXED = JOIN PREFIXED_1 BY (GTS::classId, GTS::labelsId, prefixCell2) LEFT OUTER, TAILS BY (classId, labelsId, cell);
  
  --
  -- Fuse head + tail + GTS
  --
  $GTS = FOREACH PREFIXED GENERATE FLATTEN(Fuse(TOBAG($6,$10,$14,$18,$22)));
};


/**
* Extract an object (element) from one level of the stack.
*
* @param One stack level that contains an element : { (scriptId: long,inputId: long,level: int,value: {(obj: XX)})
*
* @return (obj: XX)
*
*/
DEFINE GetObjectFromStackLevel(STACKLEVEL) RETURNS OBJ {

FLAT1 = FOREACH $STACKLEVEL GENERATE FLATTEN($0);

FLAT2 = FOREACH FLAT1 GENERATE FLATTEN($3);

$OBJ = FOREACH FLAT2 GENERATE FLATTEN($0) AS obj;

};

/**
* Extract an object (element) from the stack at one level.
*
* @param level of the stack at which the object is located
* @param the full stack : { (scriptId: long,inputId: long,level: int,value: {(obj: XX)}) }
*
* @return (obj: XX)
*
*/
DEFINE GetObjectFromStack(INDEX, STACK) RETURNS OBJ {

STACKLEVEL = FOREACH $STACK GENERATE GetFromStack($INDEX, $0);

$OBJ = GetObjectFromStackLevel(STACKLEVEL);

};