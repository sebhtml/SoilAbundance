
# Needs Hadoop 2.3.0-cdh5.1.0

mkdir -p classes

javac \
    -classpath `hadoop classpath` \
    -d classes BigMatrixIngestor.java 

jar -cvf ingestor.jar -C classes/ .
