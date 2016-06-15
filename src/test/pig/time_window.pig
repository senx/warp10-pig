REGISTER warp10-pig.jar;

REGISTER $mc2;

IMPORT '$mc2_macros';

DEFINE TicksBoundary io.warp10.pig.TicksBoundary();
DEFINE GenIds io.warp10.pig.GenIds();

gtsWrappers = LOAD '$input' USING BinStorage() AS (encoded: bytearray);
DESCRIBE gtsWrappers;

gtsTicks = FOREACH gtsWrappers GENERATE FLATTEN(TicksBoundary(encoded)) AS (classId,labelsId,firstTick,lastTick);
DESCRIBE gtsTicks;

gtsTicksGroup = GROUP gtsTicks ALL;
DESCRIBE gtsTicksGroup;

gtsMin = FOREACH gtsTicksGroup {
    gtsOrder = ORDER gtsTicks BY firstTick ASC;
    min = LIMIT gtsOrder 1;
    GENERATE FLATTEN(min);
};

DESCRIBE gtsMin;

gtsMax = FOREACH gtsTicksGroup {
    gtsOrder = ORDER gtsTicks BY lastTick DESC;
    max = LIMIT gtsOrder 1;
    GENERATE FLATTEN(max);
};

DESCRIBE gtsMax;

firstLastTicksGroup = COGROUP gtsMin BY (classId,labelsId), gtsMax BY (classId,labelsId);
DESCRIBE firstLastTicksGroup;

firstLastTicks = FOREACH firstLastTicksGroup GENERATE gtsMin.firstTick,gtsMax.lastTick;
DESCRIBE firstLastTicks;

DUMP firstLastTicks;

