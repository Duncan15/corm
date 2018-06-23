package corm;

import com.mysql.cj.api.mysqla.result.Resultset;
import util.CopyUtil;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.*;

public class Csession {
    private Connection conn;
    private PreparedStatement statement;
    public Csession(Connection connection){
        this.conn=connection;
    }
    public void exit(){
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
        ResultSet resultSet=null;
        Map<String,TreeSet<Integer>> columnInfo=null;
        List<T> ans=new ArrayList<>();
        try{
            resultSet=statement.getResultSet();
            columnInfo=storeColumnInfo(resultSet);
            debugResultSetMetaData(resultSet);
        }catch (SQLException e){
            Corm.cormLogger.error("error happen when getting result set",e);
            return ans;
        }
        try{
            while(resultSet.next()){
                T row=inject(cl,resultSet, CopyUtil.copy(columnInfo,300));
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
        ResultSet resultSet=null;
        Map<String,TreeSet<Integer>> columnInfo=null;
        T ans=null;
        try{
            resultSet=statement.getResultSet();
            columnInfo=storeColumnInfo(resultSet);
            debugResultSetMetaData(resultSet);
        }catch (SQLException e){
            Corm.cormLogger.error("error happen when getting result set",e);
            return ans;
        }
        try{
            if(resultSet.first()) ans=inject(cl,resultSet,CopyUtil.copy(columnInfo,300));
        }catch (SQLException e){
            Corm.cormLogger.error("error happen when iterate the result",e);
        }finally {
            closeResultSet(resultSet);
        }
        return ans;
    }
    private Map<String,TreeSet<Integer>> storeColumnInfo(ResultSet resultSet){
        Map<String,TreeSet<Integer>> columnInfo=new HashMap<>();
        if(resultSet!=null){
            try{
                ResultSetMetaData metaData=resultSet.getMetaData();
                int count=metaData.getColumnCount();
                for(int i=1;i<=count;i++){
                    String curLabel=metaData.getColumnLabel(i);
                    if(columnInfo.containsKey(curLabel)){
                        columnInfo.get(curLabel).add(i);
                    }else {
                        TreeSet<Integer> innerSet=new TreeSet<>();
                        innerSet.add(i);
                        columnInfo.put(curLabel,innerSet);
                    }
                }
            }catch (SQLException e){
                e.printStackTrace();
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
    private<T> T inject(Class<T> cl,ResultSet resultSet,Map<String ,TreeSet<Integer>> columnInfo){
        T ans=null;
        try{
            ans=cl.newInstance();
        }catch (InstantiationException e){
            Corm.cormLogger.error("error happen when create orm object",e);
            return ans;
        }catch (IllegalAccessException e){
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
                    TreeSet<Integer> innerSet=columnInfo.get(e.getName());
                    if(innerSet==null||innerSet.first()==null){
                        if(e.getType().isPrimitive()){
                            e.set(ans,0);
                        }else {
                            e.set(ans,null);
                        }
                    }else {
                        int index= innerSet.pollFirst();
                        Object value=resultSet.getObject(index);
                        if(value==null){
                            if(e.getType().isPrimitive()){
                                e.set(ans,0);
                            }else {
                                e.set(ans,null);
                            }
                        }else {
                            value=e.getType().getConstructor(String.class).newInstance(value.toString());
                            e.set(ans,value);
                        }

                    }
                }catch (SQLException err){
                    Corm.cormLogger.error("error happen when get the field from result set:"+e.getName(),err);
                }catch (IllegalAccessException err){
                    Corm.cormLogger.error("error happen when access the field"+e.getName(),err);
                }catch (NoSuchMethodException err){
                    Corm.cormLogger.error("error happen when get the field constructor"+e.getName(),err);
                }catch (InstantiationException err){
                    Corm.cormLogger.error("error happen when init the field"+e.getName(),err);
                }catch (InvocationTargetException err){
                    Corm.cormLogger.error("error happen and i don't know why");
                }
            }
        }
        return ans;
    }
    private void closeResultSet(ResultSet r){
        try{
            r.close();
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

}
