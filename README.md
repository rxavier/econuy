# ECON-UY

Data retrieval and processing for the Uruguayan economy.

## What

This project aims at simplifying gathering, processing and visualization of Uruguay economic statistics. Data is retrieved from government sources and can be transformed in several ways (converting to dollars, calculating rolling averages, resampling to other frequencies, etc.).

## Why

Life is short. Visiting a dozens of web pages and downloading hundreds of Excel files takes time.

## How

Muscling through with Pandas. For now, the project lives in a very disorganized manner in Python scripts.

## What next

* Automating retrieval for many more datasets. For now it's just national accounts.
* Handling everything with column multi-indexes really doesn't seem like the best way to go around this. Maybe classes, maybe SQL.
* Automating data updates.
* Showing it to the world. Ideally I'd like this to be hosted somewhere for people to use.
