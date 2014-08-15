sbt package
scp target/scala-2.10/simple-project_2.10-1.0.jar ganeao@dco-head001.dco.ethz.ch:
ssh ganeao@dco-head001.dco.ethz.ch 'for i in {33..40}; do   scp *.jar root@dco-node0$i.dco.ethz.ch:spark/; done'
