# MapReduce exercises
Sample of exercises solved using Hadoop MapReduce

## Word count
Counts the number of each word in any text file

## First char count
Same as before but for each first letter of each word

## Inverted index
Given any text file it builds an inverted index

## Log parser
For a any given file with log records like:

_10.223.157.186 - - [15/Jul/2009:14:58:59 -0700] "GET / HTTP/1.1" 403 202_

It first filters those whose status code isn't 2XX.

Then we count the number of requests for each URL.
Then we count number of request for each month for year 2010.
Finally the count of requests for each hour for year 2010.

## Friends recommender
For a any given file with lines like:

_ID \t ID1,ID2,...,IDn_

We suggest a top 10 of users for each user who aren't already friend and ordered by their number of friends in common and their IDs.

## Bakery
Given a dataset which consists of the following files:

   - customers.csv : information about the bakery's customers
   - goods.csv     : information about the baked goods offered for sale by the bakery
   - items.csv     : itemized reciept infromation for purchases
   - reciepts.csv  : general reciept information for purchases

We count the total amount of money spent by each user, which users have bought more than once per day and the list of those dates and for each product the total number of units sold.
