# My thoughts on parsing diem chuan.json file

## Problems
There are some problems:
- The data is nested
- each row consist of many majors
- each row is an university

## Goal
I want to split that big json file into files representing each university

Steps:

For each row, we create a new dataframe from the array of points and save it.
