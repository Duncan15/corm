package corm;

import util.CopyUtil;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;

public class Csession {
    private Connection conn;
    private Map<String,Queue<Integer>> columnInfo;

    public PreparedStatement statement;
    public ResultSet resultSet;
    /*
    usually this method won't be invoke by user
     */
    public Csession(Connection connection){
        this.conn=connection;
    }
    public void exit(){
        closeResultSet(resultSet);
        try {
            if(statement!=null){
                statement.close();
            }
        }catch (SQLException e){
            Corm.cormLogger.error("error happen when close statement",e);
        }

        ConnectionPool.returnConn(conn);
    }
    public Csession sql(String sql,Object... param)throws SQLException{
        /*
        if can't return keys,this param will be ignore,don't worry about this
         */
        statement=conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        setParam(param);
        statement.execute();
        return this;
    }
    public<T> List<T> find(Class<T> cl){
        List<T> ans=new ArrayList<>();

        try{
            resultSet=statement.getResultSet();
            columnInfo=storeColumnInfoToQueue(resultSet);
            debugResultSetMetaData(resultSet);

            while(resultSet.next()){
                //T row=inject(cl,resultSet, CopyUtil.copy(columnInfo,300));
                T row=inject(cl,resultSet,columnInfo);
                if(row!=null){
                    ans.add(row);
                }
            }
        }catch (SQLException e){
            Corm.cormLogger.error("error happen when iterate the result",e);
        }finally {
            closeResultSet(resultSet);
        }
        return ans;
    }
    public<T> T get(Class<T> cl){

        T ans=null;

        try{
            resultSet=statement.getResultSet();
            columnInfo=storeColumnInfoToQueue(resultSet);
            debugResultSetMetaData(resultSet);

            if(resultSet.first()) ans=inject(cl,resultSet,columnInfo);
        }catch (SQLException e){
            Corm.cormLogger.error("error happen when iterate the result",e);
        }finally {
            closeResultSet(resultSet);
        }
        return ans;
    }
    public List<Long> insert(){
        return _update();
    }
    public List<Long> update(){
        return _update();
    }
    public List<Long> delete(){
        return _update();
    }
    private List<Long> _update(){
        List<Long> ans=new ArrayList<>();
        try{
            resultSet=statement.getGeneratedKeys();
            if(resultSet.next()){
                ans.add(resultSet.getLong(1));
            }
        }catch (SQLException e){
            Corm.cormLogger.error("error happen when get generated keys ",e);
        }
        return ans;
    }

    public boolean begin(){
        return setTransaction(Type.CommitType.NO_AUTO_COMMIT);
    }
    private boolean setTransaction(Type.CommitType type){
        try{
            if(type== Type.CommitType.AUTO_COMMIT){
                this.conn.setAutoCommit(true);
            }else if (type==Type.CommitType.NO_AUTO_COMMIT){
                this.conn.setAutoCommit(false);
            }else {
                return false;
            }

        }catch (SQLException e){
            Corm.cormLogger.error("a database access error occurs" +
                    " or this method is called on a closed connection",e);
            return false;
        }
        return true;
    }

    //be called only not in auto-commit mode
    public boolean commit(){
        try{
            this.conn.commit();
        }catch (SQLException e){
            Corm.cormLogger.error("a database access error occurs" +
                    " or this method is called on a closed connection or this connection object is in auto-commit mode",e);
            return false;
        }
        finally {
            setTransaction(Type.CommitType.AUTO_COMMIT);
        }
        return true;
    }

    //be called only not in auto-commit mode
    public boolean rollback(){
        try {
            this.conn.rollback();
        }catch (SQLException e){
            Corm.cormLogger.error("a database access error occurs" +
                    " or this method is called on a closed connection or this connection object is in auto-commit mode",e);
            return false;
        }
        finally {
            setTransaction(Type.CommitType.AUTO_COMMIT);
        }
        return true;
    }

    private Map<String,Queue<Integer>> storeColumnInfoToQueue(ResultSet resultSet){
        Map<String,Queue<Integer>> columnInfo=new HashMap<>();
        if(resultSet!=null){
            try{
                ResultSetMetaData metaData=resultSet.getMetaData();
                int count=metaData.getColumnCount();
                for(int i=1;i<=count;i++){
                    String curLabel=metaData.getColumnLabel(i);
                    if(columnInfo.containsKey(curLabel)){
                        columnInfo.get(curLabel).add(i);
                    }else {
                        Queue<Integer> queue=new ArrayDeque<>();
                        queue.add(i);
                        columnInfo.put(curLabel,queue);
                    }
                }
            }catch (SQLException e){
                Corm.cormLogger.error("error happen when store column info",e);
            }
        }
        return columnInfo;
    }
    public String debugResultSetMetaData(ResultSet resultSet){
        if(resultSet!=null){
            try{
                ResultSetMetaData metaData=resultSet.getMetaData();
                int len=metaData.getColumnCount();
                for(int i=1;i<=len;i++){

                }
            }catch (SQLException e){
                e.printStackTrace();
            }

        }
        return null;
    }
    /*
    inner method: responsible for inject data into struct
    syntax: all the field is public, and if is extend field ,set the name sufix as _ext ,like bellow
    class sample{
        public User sample1_ext;
        public Account sample2_ext;
    }

    if is original field, set it's name the same with the one from the sql result
     */
    private<T> T inject(Class<T> cl,ResultSet resultSet,Map<String ,Queue<Integer>> columnInfo){
        T ans=null;
        try{
            ans=cl.newInstance();
        }catch (Exception e){
            Corm.cormLogger.error("error happen when create orm object",e);
            return ans;
        }

        Field[] fields=cl.getDeclaredFields();
        for(Field e:fields){
            if(e.getName().endsWith("_ext")){
                try{
                    e.set(ans,inject(e.getType(),resultSet,columnInfo));
                }catch (IllegalAccessException err){
                    Corm.cormLogger.error("error happen when access the field"+e.getName(),err);
                }
            }else {
                try{
                    Queue<Integer> queue=columnInfo.get(e.getName());
                    if(queue==null||queue.size()==0){
                        setDefault(e,ans);
                    }else {
                        int index=queue.poll();
                        queue.add(index);
                        Object value=resultSet.getObject(index,e.getType());//the type mapping is absolutely equal to the specification in JDBC and no other localize
                        if(value==null){//the value will be null if the value in db is null
                            setDefault(e,ans);
                        }else {
                            e.set(ans,value);
                        }
                    }
                }catch (SQLException err){
                    Corm.cormLogger.error("error happen when get the field from result set:"+e.getName(),err);
                }catch (IllegalAccessException err){
                    Corm.cormLogger.error("error happen when access the field"+e.getName(),err);
                }
            }
        }
        return ans;
    }
    private void closeResultSet(ResultSet r){
        try{
            if(r!=null){
                r.close();
            }
        }catch (SQLException e){
            Corm.cormLogger.error("error happen when close result set",e);
        }
    }
    /*
	param here should fit the order in the sql
	 */
    private void setParam(Object... param) throws SQLException {
        for(int i=1;i<=param.length;i++){
            int index=i-1;
            Object cur=param[index];
            if(cur instanceof Long){//long
                statement.setLong(i,(Long)cur);
            }else if(cur instanceof Integer) {//int
                statement.setInt(i,(Integer)cur);
            }else if(cur instanceof Byte){//byte
                statement.setByte(i,(Byte)cur);
            }else if(cur instanceof Float) {//float
                statement.setFloat(i,(Float)cur);
            }else if(cur instanceof Double){//double
                statement.setDouble(i,(Double)cur);
            }else if(cur instanceof Short){//short
                statement.setShort(i,(Short)cur);
            }else if(cur instanceof String){//string
                statement.setString(i,(String)cur);
            }else if(cur instanceof byte[]){//byte[]
                statement.setBytes(i,(byte[])cur);
            }else if(cur instanceof Timestamp){//timestamp
                statement.setTimestamp(i,(Timestamp) cur);
            }else if(cur==null){
                statement.setNull(i,Types.INTEGER);
            }
        }
    }
    private void setDefault(Field field,Object obj){
        try {
            if(field.getType().isPrimitive()){
                field.set(obj,0);
            }else {
                field.set(obj,null);
            }
        }catch (IllegalAccessException e){
            Corm.cormLogger.error("error happen when set default value",e);
        }

    }

}
