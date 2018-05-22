package myutil;
import java.util.Stack;
import java.util.function.Supplier;
import java.io.File;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.SQLType;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.apache.log4j.Logger;

public class PollingThread extends Thread {

  static class Field<T> {
    int index; String name; SQLType type; T value;
    Field(Stack<Field<T>> l,String name,SQLType type,T value) {
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
  Stack<Field<Object>> staticFields;
  Stack<Field<Supplier<Object>>> updatableFields;
  Connection conn;
  String sql_create, sql_init, sql_update, sql_error;
  PreparedStatement stmt_update, stmt_error;
  boolean stopRequested;
  final long startTime;
  Supplier<Double> elapsed;
  Logger logger = Logger.getRootLogger();

  public PollingThread (String path) throws Exception { this(path,1.,3); }
  public PollingThread (String path,double interval) throws Exception { this(path,interval,3); }
  public PollingThread (String path,double interval,int maxerror) throws Exception {
    super();
    Class.forName("org.sqlite.JDBC");
    this.path = new File(path).getAbsolutePath();
    this.interval = (long) (1000.*interval);
    this.maxerror = maxerror;
    staticFields = new Stack<>();
    updatableFields = new Stack<>();
    startTime = System.currentTimeMillis();
    elapsed = ()->(System.currentTimeMillis()-startTime)/1000.;
    addField("started",JDBCType.TIMESTAMP,startTime);
    addField("pid",0);
    addField("elapsed",elapsed);
    addField("error",JDBCType.VARCHAR,(Supplier<String>)(()->null));
  }

  public void addField(String name,Object value) throws Exception {
    addField(name,guesstype(value),value);
  }
  public void addField(String name,Supplier value) throws Exception {
    addField(name,guesstype(value.get()),value);
  }
  public void addField(String name,SQLType type,Object value) throws Exception {
    checknotalive(); new Field<Object>(staticFields,name,type,value);
  }
  @SuppressWarnings("unchecked")
  public void addField(String name,SQLType type,Supplier value) throws Exception {
    checknotalive(); new Field<Supplier<Object>>(updatableFields,name,type,(Supplier<Object>)value);
  }
  void checknotalive() throws Exception {
    if (isAlive()) { throw new Exception("Attempt to add a field to a live polling thread"); }
  }

  public void finalise_and_start() throws Exception {
    Stack<String> sql_create_ = new Stack<>();
    for (Field<Object> f: staticFields) { sql_create_.push(f.name+" "+f.type.getName()); }
    for (Field<Supplier<Object>> f: updatableFields) { sql_create_.push(f.name+" "+f.type.getName()); }
    sql_create = "CREATE TABLE Status ("+String.join(", ",sql_create_)+")";
    Stack<String> sql_init_1 = new Stack<>(); Stack<String> sql_init_2 = new Stack<String>();
    for (Field<Object> f: staticFields) { sql_init_1.push(f.name); sql_init_2.push("?"); }
    sql_init = "INSERT INTO Status ("+String.join(",",sql_init_1)+") VALUES ("+String.join(",",sql_init_2)+")";
    Stack<String> sql_update_ = new Stack<>();
    for (Field<Supplier<Object>> f: updatableFields) { sql_update_.push(f.name+"=?"); }
    sql_update = "UPDATE Status SET "+String.join(",",sql_update_);
    Stack<String> sql_error_ = new Stack<>();
    for (Field<Supplier<Object>> f: updatableFields) { if (!f.name.equals("error")&&!f.name.equals("elapsed")) { sql_update_.push(f.name+"=NULL"); } }
    sql_error = "UPDATE Status SET elapsed=?, error=?"+String.join(",",sql_error_);
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
        for (Field<Object> f: staticFields) { stmt_init.setObject(f.index,f.value); }
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
          for (Field<Supplier<Object>> f: updatableFields) { stmt_update.setObject(f.index,f.value.get()); }
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

  synchronized public void shutdown() throws Exception { stopRequested = true; join(); }

}
