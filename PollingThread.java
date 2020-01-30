package myutil;
import java.util.Stack;
import java.util.function.Supplier;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.SQLType;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.apache.log4j.Logger;

/**
 * Instances of this class are objects which can encapsulate any piece of code.
 * It starts a separate thread which invokes a set of probes at regular intervals.
 * The result of the probes are stored in a sqlite3 database.
 * Only one result is available at a time (a new probe deletes the previous one).
 * One-shot probes are invoked once at the beginning then never updated.
 */
public class PollingThread extends Thread {

  static class Probe<T> {
    int index; String name; SQLType type; T value;
    Probe(Stack<Probe<T>> l,String name,SQLType type,T value) {
      l.push(this); this.index = l.size();
      this.name = name; this.type = type; this.value=value;
    }
  }
  static SQLType guesstype(Object v) {
    return (v instanceof String?JDBCType.VARCHAR:(v instanceof Integer||v instanceof Long)?JDBCType.INTEGER:(v instanceof Float||v instanceof Double)?JDBCType.FLOAT:JDBCType.BLOB);
  }

  String path;
  long interval;
  int maxerror;
  Stack<Probe<Object>> staticProbes;
  Stack<Probe<Supplier<Object>>> updatableProbes;
  Connection conn;
  String sql_create, sql_init, sql_update, sql_error;
  PreparedStatement stmt_update, stmt_error;
  boolean stopRequested;
  final long startTime;
  Supplier<Double> elapsed;
  Logger logger = Logger.getRootLogger();

  /**
   * Constructor of the class. Defaults the <code>interval</code> parameter to <code>1.</code> and <code>maxerror</code> parameter to <code>3</code>.
   * @see PollingThread(String,double,int)
   */
  public PollingThread (String path) throws Exception {
    this(path,1.,3);
  }
  /**
   * Constructor of the class. Defaults the <code>maxerror</code> parameter to <code>3</code>.
   * @see PollingThread(String,double,int)
   */
  public PollingThread (String path,double interval) throws Exception {
    this(path,interval,3);
  }
  /**
   * Constructor of the class.
   * @param path the path to the sqlite database for storing the probe results
   * @param interval length of interval (in seconds) between two probe campaigns
   * @param maxerror number of consecutive probe campaigns allowed to fail before giving up the whole polling
   */
  public PollingThread (String path,double interval,int maxerror) throws Exception {
    super();
    Class.forName("org.sqlite.JDBC");
    this.path = new File(path).getAbsolutePath();
    this.interval = (long) (1000.*interval);
    this.maxerror = maxerror;
    staticProbes = new Stack<>();
    updatableProbes = new Stack<>();
    startTime = System.currentTimeMillis();
    elapsed = ()->(System.currentTimeMillis()-startTime)/1000.;
    addProbe("started",JDBCType.TIMESTAMP,startTime);
    addProbe("pid",ManagementFactory.getRuntimeMXBean().getName());
    addProbe("elapsed",elapsed);
    addProbe("error",JDBCType.VARCHAR,(Supplier<String>)(()->null));
  }

  /**
   * Declares a one-shot probe to add to the polling thread. Guesses the <code>type</code> parameter from <code>value</code>.
   * @see addProbe(String,SQLType,Object)
   */
  public void addProbe(String name,Object value) throws Exception {
    addProbe(name,guesstype(value),value);
  }
  /**
   * Declares a probe to add to the polling thread. Guesses the <code>type</code> parameter from a call to <code>value</code>.
   * @see addProbe(String,SQLType,Supplier)
   */
  public void addProbe(String name,Supplier value) throws Exception {
    addProbe(name,guesstype(value.get()),value);
  }
  /**
   * Declares a one-shot probe to add to the polling thread.
   * @param name name of the probe, used as column name in the sqlite database holding the results
   * @param type SQL type of the column
   * @param value value to store at the (unique) invocation of the probe
   */
  public void addProbe(String name,SQLType type,Object value) throws Exception {
    checknotalive(); new Probe<Object>(staticProbes,name,type,value);
  }
  /**
   * Declares a probe to add to the polling thread.
   * @param name name of the probe, used as column name in the sqlite database holding the results
   * @param type SQL type of the column
   * @param value supplier of the value to store, invoked at each interval
   */
  @SuppressWarnings("unchecked")
  public void addProbe(String name,SQLType type,Supplier value) throws Exception {
    checknotalive(); new Probe<Supplier<Object>>(updatableProbes,name,type,(Supplier<Object>)value);
  }
  void checknotalive() throws Exception {
    if (isAlive()) { throw new Exception("Attempt to add a probe to a live polling thread"); }
  }

  /**
   * Starts the polling thread. Must be called just before the piece of code block to poll, normally encapsulated in a <code>try ... finally</code> construct.
   */
  public void finalise_and_start() throws Exception {
    Stack<String> sql_create_ = new Stack<>();
    for (Probe<Object> f: staticProbes) { sql_create_.push(f.name+" "+f.type.getName()); }
    for (Probe<Supplier<Object>> f: updatableProbes) { sql_create_.push(f.name+" "+f.type.getName()); }
    sql_create = "CREATE TABLE Status ("+String.join(", ",sql_create_)+")";
    Stack<String> sql_init_1 = new Stack<>(); Stack<String> sql_init_2 = new Stack<String>();
    for (Probe<Object> f: staticProbes) { sql_init_1.push(f.name); sql_init_2.push("?"); }
    sql_init = "INSERT INTO Status ("+String.join(",",sql_init_1)+") VALUES ("+String.join(",",sql_init_2)+")";
    Stack<String> sql_update_ = new Stack<>();
    for (Probe<Supplier<Object>> f: updatableProbes) { sql_update_.push(f.name+"=?"); }
    sql_update = "UPDATE Status SET "+String.join(",",sql_update_);
    Stack<String> sql_error_ = new Stack<>();
    sql_error_.push("elapsed=?, error=?");
    for (Probe<Supplier<Object>> f: updatableProbes) { if (!f.name.equals("error")&&!f.name.equals("elapsed")) { sql_error_.push(f.name+"=NULL"); } }
    sql_error = "UPDATE Status SET "+String.join(",",sql_error_);
    start();
  }

  void open() throws SQLException {
    new File(path).delete();
    conn = DriverManager.getConnection("jdbc:sqlite:"+path);
    try {
      conn.setAutoCommit(false);
      Statement stmt_create = conn.createStatement();
      try { stmt_create.execute(sql_create); }
      finally { stmt_create.close(); }
      PreparedStatement stmt_init = conn.prepareStatement(sql_init);
      try {
        for (Probe<Object> f: staticProbes) { stmt_init.setObject(f.index,f.value); }
        stmt_init.execute();
      }
      finally { stmt_init.close(); }
      conn.commit();
      stmt_update = conn.prepareStatement(sql_update);
      stmt_error = conn.prepareStatement(sql_error);
    }
    catch (SQLException exc) { close(); throw exc; }
  }

  void close() {
    try { conn.close(); }
    catch (SQLException exc) {}
  }

  @Override
  public void run() {
    try { open(); }
    catch (Exception exc) { exc.printStackTrace(); logger.warn("Unable to open status file "+path); return; }
    int error = 0;
    boolean ongoing = true;
    while (ongoing) {
      try { sleep(interval); }
      catch (Exception exc) { close(); break; }
      ongoing = !stopRequested;
      try {
        try {
          for (Probe<Supplier<Object>> f: updatableProbes) { stmt_update.setObject(f.index,f.value.get()); }
          stmt_update.execute();
          conn.commit();
          error = 0;
          continue;
        }
        catch (SQLException exc) { throw exc; }
        catch (Exception exc) {
          if (++error<maxerror) {
            stmt_error.setDouble(1,elapsed.get());
            stmt_error.setString(2,exc.toString());
            stmt_error.execute();
            conn.commit();
            continue;
          }
          else { exc.printStackTrace(); close(); }
        }
      }
      catch (SQLException exc) {
        if (++error<maxerror) {
          close();
          try { open(); continue; }
          catch (SQLException exc2) { exc2.printStackTrace(); }
        }
        else { exc.printStackTrace(); }
      }
      logger.warn("Unable to record status in "+path+" (giving up after "+maxerror+" errors)");
      return;
    }
    try {
      Statement stmt_final = conn.createStatement();
      try { stmt_final.execute("PRAGMA user_version = 1"); }
      finally { stmt_final.close(); }
      conn.commit();
    }
    catch (Exception exc) { exc.printStackTrace(); }
    close();
  }

  /**
   * Ends the polling thread. Must be called just after the code block to poll, preferably in the <code>finally</code> clause encapsulating it.
   */
  synchronized public void shutdown() throws Exception { stopRequested = true; join(); }

}
