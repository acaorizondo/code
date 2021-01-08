/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cu.uh.ddi.ejemplo_spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;

/**
 *
 * @author Arturo
 * preparación de datos específica para el fichero de datos "Colic Horse"
 */
public class DataPreparation {
    
    
    public static Dataset<Row> dataPreparation(Dataset<Row> primary_data) {
    
       long cant_filas_inicial = primary_data.count();
       System.out.println("El dataframe cargado tiene "+cant_filas_inicial+" filas");
       System.out.println("Esquema del dataframe y sus 10 primeras filas");
       primary_data.printSchema();
       primary_data.show(10);
       
       //se define outcome como target para predecir si el caballo vivió o murió
       System.out.println("Posibles valores del atributo predictor");
       primary_data.select("outcome").distinct().show();
      
       /*
       se eliminan los que sufrieron eutanasia para dejar solo los que vivieron y murieron de forma natural 
       y así trabajar con un problema de predicción de clasificación binaria 
       */
       Dataset<Row> ds_1 = primary_data.filter(col("outcome").equalTo(1).or(col("outcome").equalTo(2)));
       long cant_filas_resultantes = ds_1.count();
       long filas_eliminadas = cant_filas_inicial-cant_filas_resultantes;
       System.out.println("Filas eliminadas "+filas_eliminadas);
       
       /*
       se eliminan los atributos Hospital Number, cp_data (declarado como no significativo en el estudio) 
       y las lesiones 2 y 3 (es menos frecuente que ocurra más de una lesión) que se asume no son relevantes 
       para la clasificación
       */
       Dataset<Row> ds_2 = ds_1.select(col("surgery?"),col("Age"),col("rectal temperature").cast("Double"),col("pulse"),
                                       col("respiratory rate"),col("temperature of extremities"),col("peripheral pulse"),col("mucous membranes"),
                                       col("capillary refill time"),col("pain"),col("peristalsis"),col("abdominal distension"),
                                       col("nasogastric tube"),col("nasogastric reflux"),col("nasogastric reflux PH"),col("rectal examination - feces"),
                                       col("abdomen"),col("packed cell volume"),col("total protein"),col("abdominocentesis appearance"),
                                       col("abdomcentesis total protein"),col("surgical lesion?"),col("lesion_1"),col("outcome").cast("Integer").as("label"));
       
        return ds_2.na().drop();
        
    }
    
}
