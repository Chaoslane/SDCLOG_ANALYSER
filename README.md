# sdclog-analyser-mr
Analysis SDClog command:
hadoop jar sdclog-analyser-mr.jar com.udbac.hadoop.mr.LogAnalyserMapper \
-Dmapreduce.job.reduces=3 -Dfields=dcsid -files udbacIPtransArea.csv,udbacIPtransSegs.csv \
input output

