# Project-Work

TopCharts Project - This project is to display (save) Top Chart Numbers and Top Chart Numbers State-wise.

The script needs 4 parameters - Chart Type (chart or state_chart), Limit (number of chart tracks), Input File Path, Output Directory Path.

It takes Json file as an input, reads the contents, (It does some general input validation checks while accepting the parameters) and depending on the type of chart and number of tracks, it will calculate top chart tracks (overall or statewise depending on the input) and store it in Output Directory Path. (Currently, coalesce has been used to store all the output rows in one file, however, it can be removed to store data in different part files if the input data is huge.)

Pre-requisites: It expects Java 8, Spark 2.1.1, Scala 2.11 versions and sbt installed.

To run the script:

(from the main directory where src dir, build.sbt file, etc. are present, run below command to create jar file) ->
sbt assembly

spark-submit --class com.ctm.Main --master local[*] <jar file name path> <chart_type> <limit> <Input File path> <Output Dir>

For example - 
(If running from the directory where the jar file is present)

For chart as an input ->
spark-submit --class com.ctm.Main --master local[4] ./ShazamEngine.jar chart 10 ~/gen_project/input_file/shazamtag.json ~/gen_project/output_file/
 
For state_chart as an input ->
spark-submit --class com.ctm.Main --master local[4] ./ShazamEngine.jar chart 10 ~/gen_project/input_file/shazamtag.json ~/gen_project/output_file/
