--
-- This is an example Pig script which demonstrates how to use
-- WarpScript from within Pig.
--
-- The philosophy behind the use of WarpScript in Pig is to process
-- tuples from relations with a UDF (WarpScriptRun) which executes
-- WarpScript code on a stack populated with input.
--
-- The output of the WarpScriptRun UDF is always a singleton containing a tuple.
-- This tuple contains the elements which were on the stack after the WarpScript
-- code execution, the top of the stack being the first element of the tuple.
--
-- WarpScriptRun will attempt to convert objects at the various levels
-- of the stack to their Pig counterpart.
-- Lists will become tuples, maps will become maps, etc.
-- To generate a bag, one should leave a vector (a list converted using ->V)
-- on the stack.
--
-- Note that the output of WarpScriptRun being always a tuple containing atuple,
-- you may want to wrap the call to WarpScriptRun in a call to FLATTEN to get rid
-- of one level of nesting.
--
-- The syntax of the WarpScriptRun is:
--
--   WarpScriptRun('code', arg0, arg1, ...)
--
-- or
--
--   WarpScriptRun('@file.mc2', arg0, arg1, ...)
--
-- The second syntax will load the WarpScript code from 'file.mc2' which you
-- should 'REGISTER' in your Pig script.
--
-- Arguments arg0, ... will be put on the stack with arg0 on the top.
--

--
-- Register the Warp 10 Pig jar
--

REGISTER warp10-pig-0.0.2.jar;

--
-- Register and define the Warp 10 configuration
--

REGISTER warp10.conf;
SET warp10.config 'warp10.conf';

-- Alternatively if you do not need to configure many properties, simply set
-- the Warp 10 time units using the following line:
-- SET warp.timeunits 'us';

--
-- Define the WarpScriptRun UDF
--

DEFINE WarpScriptRun io.warp10.pig.WarpScriptRun();

--
-- We define a relation using the RangeLoadFunc to generate numbers from 0 to 9
--

SET range.splits 2;
NUMBERS = LOAD 'range://0:10' USING io.warp10.pig.RangeLoadFunc();

--
-- We will now run some WarpScript code to create a string from NUMBERS' input tuple fields.
--

STRING = FOREACH NUMBERS GENERATE WarpScriptRun('TOSTRING " [TOP IN]" + SWAP TOSTRING " [2 IN]" + "[TOP OUT]"', $0, $1);

DUMP STRING;
