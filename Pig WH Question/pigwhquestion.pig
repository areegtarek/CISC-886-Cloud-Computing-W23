question = LOAD 'comqa_train.txt'  as (data:chararray );

a = foreach question generate TOKENIZE(data) as data;

b = foreach a generate flatten(data) as data ;

c = FILTER b by (data matches 'what' OR data matches 'when' OR data matches 'where' OR data matches 'how' OR data matches 'which' OR data matches 'whom' OR data matches 'who' OR data matches 'why' OR data matches 'whose');

d= group c by data;

e= foreach d generate group, COUNT(c.data) as whcount;

dump e
