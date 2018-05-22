package myutil.demo;
import java.util.function.Supplier;
import java.sql.JDBCType;
import myutil.PollingThread;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class PollingThreadDemo {
  static class State { int current=0; }
  public static void main(String[] args) throws Exception {
    if (args.length!=1) { System.out.println("usage: <statusfile>"); System.exit(1); }
    PollingThread polling = new PollingThread(args[0]);
    final State state = new State();
    polling.addField("current",(Supplier<Integer>) ()->state.current);
    polling.addField("fortytwo",42);
    polling.finalise_and_start();
    System.out.println("Starting main loop");
    Logger.getRootLogger().setLevel(Level.WARN);
    try {
      for (int i=1;i<=60;i++) {
        state.current += 1;
        Thread.currentThread().sleep(10);
      }
    }
    finally { polling.shutdown(); }
  }
}
