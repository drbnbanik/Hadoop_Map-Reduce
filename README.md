Google nGrams
=================

Google ngrams: Google counted occurrences of words in the entire collection of their Google 
Books. There are two files: one file that reports 1 grams (single words) and the other file that 
reports 2 grams (pairs of words; one following each other in text). Note that the two files are 
only 2 sample files and not the entire data set which is too big to be stored by each one of the 
16 teams. 
The 1gram file format: word \t year \t number of occurrences \t number of volumes \t potential 
other irrelevant fields
The 2 gram file format: word \t word \t year \t number of occurrences \t number of volumes \
potential other irrelevant fields

The data represents the number of occurrences of a particular word (or pairs of words) in a 
given year in all books available by Google Books. The number of different volumes/books 
including the word (or pairs of words) is also reported. 
Write a mapreduce java routine that will for each year report the average number of volumes 
including words containing substrings: ‘nu’, ‘die’, ‘kla’

The output should be: year substring average; Example: 2000 nu 345; 1998 die 31; …
Do this in the most efficient way with a single mapreduce job. Beyond the file formats described 
above, you are not allowed to make any structural assumptions on the data; e.g., that there are 
no extra fields – some records can have extra fields. The ‘year’ column may include erroneous 
values which can be a string. If the year field is a string, the record should be discarded.
