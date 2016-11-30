--
-- This is an example of the use of the RangeLoadFunc provided
-- by Warp 10 Pig.
--
-- This pseudo load function will produce a relation containing
-- numbers in sequence.
--
-- The pseudo location has the syntax 'range://START:COUNT' and will
-- trigger the generation of numbers from START to START + COUNT - 1.
--
-- The number of splits is controlled by the 'range.splits' parameter.
--
-- RangeLoadFunc will emit tuples whith two fields, 'seqno' and 'value'.
-- 'seqno' is the sequence of each number generated, starting with 0 for
-- each split, and 'value' is the actual number generated.
--

--
-- Register the Warp 10 pig jar
--

REGISTER warp10-pig-0.0.2.jar;

--
-- Define the number of splits we want
--

SET range.splits 2;

seqno = LOAD 'range://0:100' USING io.warp10.pig.RangeLoadFunc();

DUMP seqno;
