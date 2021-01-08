/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cu.uh.ddi.spark.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author Arturo
 */
public class DataSourceFileUtil {
    
    private final Dataset<Row> data;
        
    public DataSourceFileUtil(SparkSession ss, String file_header, String original_file_location, String type_of_file){
        // convertir a csv si no lo es
        if (type_of_file.equals("data_file"))
           data = primaryData(ss, file_header, original_file_location);
        else
           data = primaryData(ss, original_file_location); 
            
    }
    
    public Dataset<Row> getDataset(){
        return data;
    }
    
    // obtener el fichero en formato csv
    public static Dataset<Row> primaryData(SparkSession ss, String csvFileLocation){
               
         Dataset<Row> ds = ss.read().option("header", true)
                                    .option("inferSchema", true)
                                    .csv(csvFileLocation);
        return ds;
    }
    
    // obtener el fichero en formato csv pero primero se transforma el fichero de datos a formato csv
    public static Dataset<Row> primaryData(SparkSession ss,
                                           String file_header,
                                           String original_file_location){
        
        // convertir fichero de datos a formato csv
         String csvFileLocation = convertFileToCsv(original_file_location, file_header);
         
         //cargar el fichero en formato csv
         Dataset<Row> ds = ss.read().option("header", true)
                                    .option("inferSchema", true)
                                    .csv(csvFileLocation);
        return ds;
    }
    
    // transformar fichero de datos a formato csv
    public static String convertFileToCsv(String dataFileName, String header){
         String csvName=dataFileName+".csv";
         
         FileWriter fw = null;
         FileReader fr = null;
         BufferedReader br = null;
         BufferedWriter bw = null;
                 
         try
         {
          fr = new FileReader (dataFileName);
          br = new BufferedReader(fr);
          fw = new FileWriter(csvName,false);
          bw = new BufferedWriter(fw);
          
          bw.write(header+"\r\n");
         
          String line = br.readLine();
          while (line!=null) {
             String n1=line.replace(" ", ",").replace("?"," ");
             String new_line = n1.substring(0, n1.length());
             bw.write(new_line+"\r\n");
             line = br.readLine();
          }  
          bw.close();
          br.close();
         }
         catch(IOException e1)
         {
          System.out.println(e1.getMessage());
         }
         finally
         {
            try{                    
             if( null != fr && null !=fw){   
                fr.close();  
                fw.close();
             }                  
            }
            catch (Exception e2){ 
             e2.printStackTrace();
            }
      }
         
             
         return csvName; 
     }
    
}
