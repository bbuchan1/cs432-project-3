# CS432 Project 3

A simple CLI tool that allows the user to view data in a MongoDB cluster.

------------------------------------------------------------------------------

## Description

This project is a CLI program that allows the user to run a set of predefined
queries on a MongoDb cluster. The cluster in question is hosted remotely on
the cloud via MongoDB's "Atlas" service.

The database itself is a collection of Pitchfork music reviews, a little more
than 18,000 to be precise. These reviews contain information on artist, album,
score, date of publication, author, review content, and more.

The predefined queries are designed to operate on the data in various
collections in order to calculate some statistics or rankings, or in order to
present the user with a human-readable output of the content.

For example, I have defined queries that calculate average scores based on
artist, genre, and year, as well as queries that generate a distribution of
scores across all reviews, and search reviews for user-given terms.

------------------------------------------------------------------------------

## Execution

To run the client, first make sure to have `pymongo` installed, then run
`python3 main.py`
