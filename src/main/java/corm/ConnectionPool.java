package corm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

//the attribute in the class must be configed when first use
public class ConnectionPool{
    private static final String driver="com.mysql.cj.jdbc.Driver";
    //set useSSL=false here can avoid the warning
    public static String connectionString;
    public static String username;
    public static String password;
    public static int poolMaxSize;//连接池最大连接数
    public static int poolMinSize;//连接池最小连接数
    private static int count;
    private static void register() {
        Corm.cormLogger.info("load mysql driver");
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            Corm.cormLogger.error("maybe you haven't add the mysql driver in your classpath",e);
        }
    }
    private static ConnectionPool connPool=null;//单例对象
    private ArrayBlockingQueue<Connection> connArray;//实际连接池  //存在于单例对象中
    private ReentrantLock lock;    //存在于单例对象中
    private ConnectionPool(){
        Corm.cormLogger.info("init ConnectionPool");
        this.count=this.poolMinSize;
        this.lock=new ReentrantLock();
        this.connArray=new ArrayBlockingQueue<>(this.poolMinSize);
        register();

        for(int i=0;i<this.poolMinSize;i++){
            try{
                Connection tmp=DriverManager.getConnection(connectionString, username, password);
                connArray.put(tmp);
            }catch (Exception e){
                Corm.cormLogger.error("error happen when init connectionPool",e);
                i--;
            }
        }
    }
    public static ConnectionPool getInstance(){
        if(connPool==null) {//克服指令排序问题
            synchronized (ConnectionPool.class) {
                if (connPool == null) {
                    connPool = new ConnectionPool();
                }
            }
        }
        return connPool;
    }
    public Connection getConn(){
        if(connArray.isEmpty()){
            Corm.cormLogger.info("array is empty now");
            lock.lock();
            if(count<poolMaxSize){
                try{
                    Corm.cormLogger.info("now have {} connections in use,create new connection",count);
                    Connection tmp=DriverManager.getConnection(connectionString, username, password);
                    count++;
                    return tmp;
                }catch (SQLException e){
                    Corm.cormLogger.error("error happen when create new connection",e);
                    return null;
                }
                finally {
                    lock.unlock();
                }
            }
            else{
                lock.unlock();
            }
        }
        Corm.cormLogger.info("now have {} connections in array,{} connections in use",connArray.size(),count);
        try{
            Connection ans= connArray.poll(3, TimeUnit.SECONDS);
            if(ans==null){
                Corm.cormLogger.warn("can't get connection from array");
                return null;
            }
            if (ans.isValid(5)){
                return ans;
            }else {
                Corm.cormLogger.warn("connection from array is invalid,create new connection");
                ans.close();
                register();
                Connection tmp=DriverManager.getConnection(connectionString, username, password);
                return tmp;
            }
        }catch (Exception e){
            Corm.cormLogger.error("error happen when create new connection",e);
            return null;
        }
    }
    public static boolean returnConn(Connection conn){
        if(conn==null)return false;
        if(connPool==null){
            try{
                conn.close();
            }catch (SQLException e){
                Corm.cormLogger.error("error happen when close connection",e);
            }finally {
                return false;
            }
        }
        connPool.lock.lock();
        if(connPool.connArray.size()>=poolMinSize){
            Corm.cormLogger.info("array size is bigger than poolMinSize,try to close connection");
            try{
                conn.close();

            }catch (SQLException e){
                Corm.cormLogger.error("error happen when close connection",e);
            }finally {
                count--;
                connPool.lock.unlock();
            }
            return false;
        }
        Corm.cormLogger.info("array size is smaller than poolMaxSize,don't close connection");
        try {
            connPool.connArray.put(conn);
            return true;
        }catch (Exception e){
            Corm.cormLogger.error("error happen when put connection into array",e);
            try {
                conn.close();
            }catch (SQLException e2){
                Corm.cormLogger.error("error happen when close connection",e2);
            }finally {
                count--;
            }
            return false;
        }finally {
            connPool.lock.unlock();
        }
    }

}