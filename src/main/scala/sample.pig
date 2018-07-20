data_name = LOAD 'word_count.text' USING PigStorage(' ') as (line: chararray);

Words = FOREACH data_name GENERATE FLATTEN(TOKENIZE(line,' ')) AS word;

Grouped = GROUP Words BY word;

wordcount = FOREACH Grouped GENERATE group, COUNT(Words);

dump wordcount;