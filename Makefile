myutil.jar: myutil/PollingThread.class myutil/demo/PollingThreadDemo.class
	jar cvf $@ myutil/*
myutil/PollingThread.class: PollingThread.java
	javac -classpath "/usr/share/java/*" -d . $<
myutil/demo/PollingThreadDemo.class: demo/PollingThreadDemo.java myutil/PollingThread.class
	javac -classpath ".:/usr/share/java/*" -d . $<
