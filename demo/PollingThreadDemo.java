package myutil.demo;
import java.util.function.Supplier;
import java.util.Random;
import java.sql.JDBCType;
import myutil.PollingThread;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class PollingThreadDemo {
  static class State { int current=0; }
  public static void main(String[] args) throws Exception {
    float xerrorrate = (float)1.;
    PollingThread polling = null;
    try {
      if (args.length!=3) { throw new RuntimeException("Cannot parse arguments"); }
      xerrorrate = Float.parseFloat(args[2]);
      polling = new PollingThread(args[0],1.,Integer.parseInt(args[1]));
    }
    catch (Exception exc) {
      exc.printStackTrace();
      System.out.println("usage: <statusfile> <maxerror> <errorrate>");
      System.exit(1);
    }
    final float errorrate = xerrorrate;
    final State state = new State();
    final Random random = new Random();
    final RuntimeException gerr = new RuntimeException("this is a generated error");
    polling.addProbe("fortytwo",42);
    polling.addProbe("current",(Supplier<Integer>) ()->state.current);
    polling.addProbe("generated_error",(Supplier<Float>) ()-> {float x = random.nextFloat(); if (x<=errorrate) { throw gerr;} else {return x;}});
    System.out.println("Starting main loop");
    Logger.getRootLogger().setLevel(Level.WARN);
    polling.finalise_and_start();
    try {
      for (int i=1;i<=60;i++) {
        state.current += 1;
        Thread.currentThread().sleep(100);
      }
    }
    finally { polling.shutdown(); }
    System.out.println("End of main loop");
  }
}
