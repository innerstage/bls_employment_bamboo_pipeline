# BLS Employment data by State and Metro Area

Getting the employment data from the BLS API has been a challenging task, I'd like to explain my findings here and explain the setbacks thoroughly.

## The Series

The data series we want come from "Employment, Hours, and Earnings - State and Metro Area" and they have the following structure:

* Prefix: Always starts with "SM".
* Seasonal Adjustment: "S" if it's seasonally adjusted and "U" when it's not.
* State: 2 digits.
* MSA: 5 digits.
* Supersector and Industry: 8 digits.
* Data Type: 2 digits. We will only use "01" for the number of employees, in thousands.

For example, the series "SMS01194606500000001" returns a seasonally adjusted ("S") time series for the total number of employees ("01" at the end) working on education and health services ("6500000") in Decatur ("19460"), Alabama ("01" at the beginning). There are mappings for each part of the Series ID, so once we know the ID, we know everything about the time series. 

## Combinations, One-Click Search and BLS API

There is a high number of combinations for the time series and not all of them have data, using the "One-Click Data Search" window on the BLS site it's possible to see which are the available series, but you have to click on a State to get the list of Areas, and then click on a Supersector to get the list of Industries. This one-click window has a limit of 200 series per request, the results are given separately so we would need to download each series manually from there. Checking the HTML after clicking on the different options, it's possible to manually construct a map of the series that have data so we can request the data from the BLS API, but since you have to click through each State and Area, it's not feasible.

The BLS API allows us to query any time series, it even tells you if the series don't have data, but it has daily thresholds:

|Service|Version 2.0|Version 1.0|
|---|:---:|:---:|
|Daily query limit|500|25|
|Series per query limit|50|25|
|Years per query limit|20|10|

I got a registration key to be able to use Version 2.0, but the daily quota of available requests is quickly met because of the combinations. This is the progress so far:

* I've used the one-click window data to map each Area to its State, manually, reducing combinations 56x.
* Manually got the statewide list of Industries by State from the one-click window, so we can reduce the combinations of Areas and Industries.
* Currently there are 446 Areas, 342 Industries and 2 options for Seasonal Adjustment. Making a total of 305,064. The manual reductions are made by state and make a total of 55,214 (About 82% reduction).
* Still, with all this, we can only request a maximum of 50 series at a time, and only 500 times a day. Making 25,000 possible requests daily.
* I'm running the download script by State to keep everything organized, so some slots might be lost in the last batch of each State, this means less than 50 series in that last batch.
* I'm using the results of each query to construct a list of the series that do exist and have data, this way, the next time we'll only query existing series and reduce the amount of necessary queries considerably.
* On any given query, the series that do exist and have data almost always amount to less than 10 out of the 50.