/*
 * @author Arturo Arias
 * 12/12/2020
 */
package cu.uh.ddi.ejemplo_spark;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;

/**
 *
 * @author Arturo Arias
 * Ejemplo de operaciones sobre un fichero csv empleando spark-sql
 */
public class CSVfileOperations {
    private static SparkSession spark;
    private static Dataset<Row> primary_data;
    
    public static void main (String[] args){
       spark = createSession();
       primary_data=primaryData(spark);
       operations();
       spark.stop();
    }
    
    public static SparkSession createSession(){
        final SparkConf conf = new SparkConf().setAppName("prueba").setMaster("local[*]");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
        
        SparkSession ss = SparkSession
                             .builder()
                             .sparkContext(sc.sc())
                             .getOrCreate();
        return ss;
    }
    
    public static Dataset<Row> primaryData(SparkSession ss){
        Dataset<Row> ds = ss.read().option("header", true)
                                   .option("inferSchema", true)
                                   .csv("D:\\Curso\\Apache Spark\\Tema 3\\covid_autopesquisa.csv");
        return ds;
    }
       
     public static void operations(){
       
       //1- Cree un Dataframe a partir del fichero e imprima en pantalla su esquema y las 10 primeras filas.
       long cant_filas_inicial = primary_data.count();
       System.out.println("El dataframe cargado tiene "+cant_filas_inicial+" filas");
       System.out.println("Esquema del dataframe y sus 10 primeras filas");
       primary_data.printSchema();
       primary_data.show(10);
       
       //2- Eliminar filas con valores ausentes
       System.out.println("Eliminar filas con valores ausentes");
       Dataset<Row> ds_1 = primary_data.na().drop();
       long cant_filas_restantes = ds_1.count();
       long cant_filas_eliminadas = cant_filas_inicial - cant_filas_restantes;
       System.out.println("Fueron eliminadas "+cant_filas_eliminadas+" filas con valores ausentes");
       ds_1.show((int)cant_filas_restantes); //muestra todo el dataframe
       
       //3 Número de filas y columnas del dataframe
       int cant_col = ds_1.schema().length();
       System.out.println("El dataframe resultante tiene "+cant_filas_restantes+" filas y "+cant_col+" columnas");
       System.out.println();
       
       //4 Muestre el nombre, edad, sexo y policlínico de las 50 primeras personas, ordenadas por la edad
       System.out.println("50 primeras personas ordenadas por edad");
       ds_1.orderBy("Edad").select(col("Nombre"),col("Edad"),col("Sexo"),col("Policlinico")).show(50);
       
       //5- personas que han sido contacto con positivos
       System.out.println("Personas que han sido contactos de casos positivos");
       Dataset<Row> ds_2 = ds_1.filter(col("Contacto con Positivo").equalTo("Si")).select(col("Nombre"));
       ds_2.show();
  
       //6- Muestre la cantidad de personas que han sido contacto con positivos
       System.out.println("Hay " + ds_2.count() + " personas que han sido contactos con positivos");
       System.out.println();
      
       //7- Muestre aquellas personas que tienen al menos una enfermedad peligrosa que pueda traer complicaciones 
       //si se enferman de la Covid-19: [Hipertensión, Insuficiencia Cardiaca, Enfermedad Coronaria, Cáncer, Diabetes, VIH]
       System.out.println("Personas con al menos una enfermedad peligrosa");
       //condiciones múltiples
       ds_1.filter(col("Insuficiencia Cardiaca").equalTo("Si").or(col("Hipertension").equalTo("Si"))
                                                              .or(col("Enfermedad Coronaria").equalTo("Si"))
                                                              .or(col("Cancer").equalTo("Si"))
                                                              .or(col("Diabetes").equalTo("Si"))
                                                              .or(col("VIH").equalTo("Si"))
                  ).select(col("Nombre"),col("Insuficiencia Cardiaca"),col("Hipertension"),
                           col("Enfermedad Coronaria"),col("Cancer"),
                           col("Diabetes"),col("VIH"))
                   .show();
                
       //8-Muestre el promedio de edad de las personas diabéticas con sexo femenino
       System.out.println("Edades de las mujeres diabéticas");
       ds_1.filter(col("Diabetes").equalTo("Si").and(col("Sexo").equalTo("F"))).select("Edad").distinct().orderBy("Edad").show();
       System.out.println("Promedio de edad de las mujeres diabéticas");
       ds_1.filter(col("Diabetes").equalTo("Si").and(col("Sexo").equalTo("F"))).select(avg("Edad").alias("Promedio de edad")).show();
       
           
       //9- Muestre las personas que al menos tengan un síntoma de la Covid-19: [Fiebre, Dolor de Garganta, 
       //Dolor de Cabeza, Tos, Falta de Aire]
       System.out.println("Personas con al menos un síntoma");
       //condiciones múltiples
       ds_1.filter(col("Fiebre").equalTo("Si").or(col("Dolor de Garganta").equalTo("Si"))
                                              .or(col("Dolor de Cabeza").equalTo("Si"))
                                              .or(col("Tos").equalTo("Si"))
                                              .or(col("Falta de aire").equalTo("Si"))
                                              .or(col("Diarrea").equalTo("Si"))
                  ).select(col("Nombre"),col("Fiebre"),col("Dolor de Garganta"),
                           col("Dolor de Cabeza"),col("Tos"),
                           col("Falta de aire"),col("Diarrea"))
                   .show();


        //10- Por sexo muestre cuantas personas han viajado.
        System.out.println("Cantidad de personas que han viajado por sexo");
        ds_1.filter(col("Han viajado").equalTo("Si")).select(col("Sexo")).groupBy("Sexo").count().show();
                   
     }   
     

    
}
