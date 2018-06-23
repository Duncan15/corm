package util;

import java.io.*;

public class CopyUtil {
    public static<T>   T copy(T obj,int cacheSize){
        T ans=null;
        ByteArrayOutputStream outputStream=new ByteArrayOutputStream(cacheSize);
        try{
            new ObjectOutputStream(outputStream).writeObject(obj);
        }catch (IOException e){
            e.printStackTrace();
        }
        ByteArrayInputStream inputStream=new ByteArrayInputStream(outputStream.toByteArray());
        try{
            ans=(T)new ObjectInputStream(inputStream).readObject();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try{
                outputStream.close();
                inputStream.close();
            }catch (IOException e){
                e.printStackTrace();
            }

        }
        return ans;
    }
}
