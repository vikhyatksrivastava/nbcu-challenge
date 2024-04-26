# Offline Assessment for Data Engineer

You are going to build a process to collect F1 drivers’ lap times, determine the average value for each drive, and order them to return the top three positions in ascending order.

This is a batch process. Your pipeline will ingest a single file which contains data in a CVS format. The input data should look something like this:
Driver | Time
----- | -----
Alonzo | 4.32
Verstrappen | 4.75
Alonzo | 4.88
Hamilton | 4.65
Alonzo | 4.38
Verstrappen | 4.55
Hamilton | 4.61
Hamilton | 4.43
Verstrappen | 4.59

You can source the data wherever you like (you can even fabricate the data if you like). Real data could be source from the F1 website, [for example](https://www.formula1.com/en/results.html/2021/races/1064/bahrain/practice-1.html). Make sure you have at least 10 drivers in your list with at least 3 lap times each.

Once you have a CSV file with drivers and times, calculate the average lap time for each driver, and sort them in ascending order.

Finally, an output with the top 3 drivers by average lap time should be generated. For each driver, include the fastest and average lap times. You are free to choose the format of the output (e.g. CSV, JSON, etc.).