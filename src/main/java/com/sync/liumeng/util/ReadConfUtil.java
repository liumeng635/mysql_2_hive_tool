package com.sync.liumeng.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ReadConfUtil {
	public static String loadConf2String(String src) {
	  InputStream stream = ReadConfUtil.class.getClassLoader().getResourceAsStream(src) ;
      StringBuffer sb = new StringBuffer() ;
      BufferedReader br = null ;
      try {
          br = new BufferedReader(new InputStreamReader(stream,"UTF-8")) ;
          String s=null ;
          while((s=br.readLine()) !=null){
              sb.append(s) ;
          }
          br.close();
      } catch (FileNotFoundException e) {
          e.printStackTrace();
      } catch (IOException e) {
    	  e.printStackTrace();
      }finally {
          if(br !=null){
              try {
                  br.close();
              } catch (IOException e) {
            	  e.printStackTrace();
              }
          }
      }
	  return sb.toString();
	}
}
