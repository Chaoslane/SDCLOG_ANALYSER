# sdclog-analyser-mr
example:
hadoop jar sdclog-analyser-mr.jar com.udbac.hadoop.mr.LogAnalyserRunner
-Dfilename.pattern=.*20170101.*
-Dfields.common=dcsid,date_time,ckid,ssid,WT.mobile
hdfs 输入路径
hdfs 输出路径


