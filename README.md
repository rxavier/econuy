# ECON-UY

Data retrieval and processing for the Uruguayan economy.

## What

This project aims at simplifying gathering, processing and visualization of Uruguay economic statistics. Data is retrieved from government sources and can be transformed in several ways (converting to dollars, calculating rolling averages, resampling to other frequencies, etc.).

## Why

Life is short. Visiting a dozens of web pages and downloading hundreds of Excel files takes time.

## How

Muscling through with Pandas. For now, the project lives in a very disorganized manner in Python scripts.

## What next

* Handling everything with column multi-indexes really doesn't seem like the best way to go around this. Maybe classes, maybe SQL.
* Automating data updates.
* Showing it to the world. Ideally I'd like this to be hosted somewhere for people to use.

#### X13 ARIMA binary

If you want to use the seasonal decomposition function in the seasonal file you'll need the X13 binary. You can get it [from here](https://www.census.gov/srd/www/x13as/x13down_pc.html) for Windows or [from here](https://www.census.gov/srd/www/x13as/x13down_unix.html) for UNIX systems. For macOS I had to compile it using the instructions found [here](https://github.com/christophsax/seasonal/wiki/Compiling-X-13ARIMA-SEATS-from-Source-for-OS-X).