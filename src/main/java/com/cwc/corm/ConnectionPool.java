package com.cwc.corm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * the attribute in the class must be configured when first use
 */
class ConnectionPool {

    private static final String driver = "com.mysql.cj.jdbc.Driver";

    /**
     * set useSSL=false here can avoid the warning
     */
    public static String connectionString;

    public static String username;

    public static String password;

    /**
     * the maximum connection number of connection pool
     */
    public static int poolMaxSize;

    /**
     * the minimum connection number of connection pool
     */
    public static int poolMinSize;

    /**
     * the number of connection in the connection pool
     */
    private static int count;

    /**
     * the singleton
     */
    private static ConnectionPool instance;

    /**
     * the connection pool in reality
     */
    private ArrayBlockingQueue<Connection> connectionPool;

    /**
     * lock to use in the singleton
     */
    private ReentrantLock lock;

    private static void register() {
        Corm.cormLogger.debug("loading mysql driver");
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            Corm.cormLogger.error("maybe you haven't add the mysql driver in your classpath", e);
        }
    }

    private ConnectionPool() {
        Corm.cormLogger.info("initiating ConnectionPool");
        this.count = this.poolMinSize;
        this.lock = new ReentrantLock();
        this.connectionPool = new ArrayBlockingQueue<>(this.poolMinSize);
        register();

        for (int i = 0; i < this.poolMinSize; i++) {
            try {
                Connection tmp = DriverManager.getConnection(connectionString, username, password);
                connectionPool.put(tmp);
            } catch (Exception e){
                Corm.cormLogger.error("error happen when initiating connectionPool", e);
                i--;
            }
        }
    }

    public static ConnectionPool getInstance() {
        if (Objects.isNull(instance)) { //overcome the directive resort problem
            synchronized (ConnectionPool.class) {
                if (Objects.isNull(instance)) {
                    instance = new ConnectionPool();
                }
            }
        }
        return instance;
    }

    public Connection getConn() {
        if (connectionPool.isEmpty()) {
            Corm.cormLogger.debug("connection pool is empty now");
            lock.lock();
            if (count < poolMaxSize) {
                try {
                    Corm.cormLogger.info("now have {} connections in use, create new connection", count);
                    Connection tmp = DriverManager.getConnection(connectionString, username, password);
                    count++;
                    return tmp;
                } catch (SQLException e) {
                    Corm.cormLogger.error("error happen when create new connection",e);
                    return null;
                } finally {
                    lock.unlock();
                }
            } else {
                lock.unlock();
            }
        }
        Corm.cormLogger.info("now have {} connections in connection pool, {} connections in use", connectionPool.size(), count);
        try {
            Connection ans = connectionPool.poll(3, TimeUnit.SECONDS);
            if (Objects.isNull(ans)) {
                Corm.cormLogger.warn("can't get connection from connection pool");
                return null;
            }
            if (ans.isValid(5)) {
                return ans;
            } else {
                Corm.cormLogger.warn("connection from connection pool is invalid, create new connection");
                ans.close();
                Connection tmp = DriverManager.getConnection(connectionString, username, password);
                return tmp;
            }
        } catch (Exception e){
            Corm.cormLogger.error("error happen when create new connection", e);
            return null;
        }
    }

    public static boolean returnConn(Connection conn) {

        Objects.requireNonNull(conn);
        if(Objects.isNull(instance)) {
            try {
                conn.close();
            } catch (SQLException e){
                Corm.cormLogger.error("error happen when close connection", e);
            } finally {
                return false;
            }
        }

        instance.lock.lock();
        if (instance.connectionPool.size() >= poolMinSize) {
            Corm.cormLogger.debug("connection pool size is bigger than poolMinSize, try to close connection");
            try {
                conn.close();

            } catch (SQLException e) {
                Corm.cormLogger.error("error happen when closing connection", e);
            } finally {
                count--;
                instance.lock.unlock();
            }
            return false;
        }
        Corm.cormLogger.debug("connection pool size is smaller than poolMaxSize, don't close connection");
        try {
            instance.connectionPool.put(conn);
            return true;
        } catch (Exception e) {
            Corm.cormLogger.error("error happen when put connection into connection pool", e);
            try {
                conn.close();
            } catch (SQLException e2) {
                Corm.cormLogger.error("error happen when closing  connection", e2);
            } finally {
                count--;
            }
            return false;
        } finally {
            instance.lock.unlock();
        }
    }

}