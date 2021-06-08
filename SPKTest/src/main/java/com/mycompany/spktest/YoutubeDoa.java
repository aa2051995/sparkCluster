package com.mycompany.spktest;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.commons.csv.CSVFormat;
import smile.*;
import smile.data.DataFrame;
import smile.data.measure.NominalScale;
import smile.io.Read;
public class YoutubeDoa {
	public  int[] encodeCategory(DataFrame df, String columnName) {
		String[] values = df.stringVector (columnName).distinct ().toArray (new String[]{});
		int[] pclassValues = df.stringVector (columnName).factorize (new NominalScale (values)).toIntArray ();
		return pclassValues;
		
		
		}
	public DataFrame readCSV(String path) {
		CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader ().withDelimiter(';');
		DataFrame df = null;
		try {
			df =  Read.csv(path,format);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("first"+df.summary ());
		df = df.select ("Name", "Pclass", "Age", "Sex", "Survived");
		System.out.println("second "+ df.summary ());
	
		return df;
		}
}
