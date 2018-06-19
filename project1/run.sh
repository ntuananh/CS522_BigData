# put the given input files in HDFS "input" folder
hadoop fs -put input /user/cloudera
wait

# Remove previous "output"
hadoop fs -rm -r /user/cloudera/output

# Run each job using the commands given below
# Part 1c
hadoop jar project.jar part1.c.WordCount /user/cloudera/input/part1/c /user/cloudera/output/part1/c
wait

# Part 1d
hadoop jar project.jar part1.d.WordCountCombiner /user/cloudera/input/part1/d /user/cloudera/output/part1/d
wait

# Part 1e
hadoop jar project.jar part1.e.AverageComputation /user/cloudera/input/part1/e /user/cloudera/output/part1/e
wait

# Part 1f
hadoop jar project.jar part1.f.AverageComputationCombiner /user/cloudera/input/part1/f /user/cloudera/output/part1/f
wait

# Part 2
hadoop jar project.jar part2.Pairs /user/cloudera/input/part2 /user/cloudera/output/part2
wait

# Part 3
hadoop jar project.jar part3.Stripes /user/cloudera/input/part3 /user/cloudera/output/part3
wait

# Part 4
hadoop jar project.jar part4.PairStripe /user/cloudera/input/part4 /user/cloudera/output/part4

# outputs are in /user/cloudera/output
