This directory consists of:

- cleaning_algorithms
In which I designed algorithms that standardize the data, fill missing values based on
key - value pairs (in case of non-numeric values) and mean values (latitude and longitude),
drops duplicates and rows that could not been filled with previous algorithms.

Also, in years 2019-2022 I have come across a lot of missing data in 'reporting_area' and
'ucr_part' columns, so I came up with idea of filling them with data from previous datasets. 
The algorithm takes the idea of key-value pairs, collects them from "better-equipped" dataset,
and places valid data in "less-equipped" dataset. 

I then collected the algorithms for the relevant years into sets. 

- clean_all 
Here I iterated through datasets and ran them through ^ those sets, 
and got all cleaned data saved to files. 
