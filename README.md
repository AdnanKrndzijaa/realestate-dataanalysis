# realestate-dataanalysis

- You must have Hadoop installed before running the commands.

## Commands
1. Open CMD as Administrator
2. ```cd C:\hadoop1.2\sbin``` - enter the sbin folder
3. ```start-all.cmd``` - start the Hadoop
4. ```hadoop fs -mkdir /input``` - create a new directory
5. ```hadoop -put /path/to/DATASETNAME.csv /input``` - upload dataset .csv file to input directory
6. Write classes, and compile the code and packages into a JAR file
7. ```hadoop jar path/to/JARFILE.jar CLASSNAME /input/DATASETNAME.csv /output_dir_CLASSNAME)``` - naming is optional, you can choose anything you want
8. ```hadoop fs -cat /output_dir_CLASSNAME/part-r-00000)``` - displaying an output
