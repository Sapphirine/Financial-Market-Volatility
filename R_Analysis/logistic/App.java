package EECSE6893.LogisticRegressionApp;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class App 
{
	public static void main( String[] args ) throws FileNotFoundException, IOException
	{

		CSVReader reader = new CSVReader(new FileReader("/home/hduser/Final_Project/logistic/train/train.csv"));
		CSVWriter writer = new CSVWriter(new FileWriter("/home/hduser/Final_Project/logistic/train/final.csv"), ',');  
		BufferedWriter bw = new BufferedWriter(new FileWriter("/home/hduser/Final_Project/logistic/train/final.txt"));
		String[] value;
	    while ((value = reader.readNext()) != null) {
	        // nextLine[] is an array of values from the line
	        System.out.println(value[0] + " " + value[1] + " " + value[2] + " "+ value[3] + " "+ value[4] + " "+ value[5]);
	        bw.write(value[0] + "," + value[1] + "," + value[2] + ","+ value[3] + ","+ value[4] + ","+ value[5]+","+ "Action"+ "\n");
	        value[reader.readNext().length-1] = "Action";
	        writer.writeNext(value);
	        break;
	    }
	    String [] nextLine;
	    nextLine = reader.readNext();
		String [] previousLine;
		String [] headernew = new String [reader.readNext().length + 1];  
		previousLine = reader.readNext();

		while ((nextLine = reader.readNext() ) != null) {
			for (int i = 0; i < headernew.length-1;i++)
			{
				headernew[i] = nextLine[i];
			}            

			if (
					Double.parseDouble(previousLine[4]) < Double.parseDouble(nextLine[4])
					)
			{
				headernew[headernew.length-1] = "SELL";
			} 
			else {
				headernew[headernew.length-1] = "BUY";
			}
			bw.write(headernew[0] + "," + headernew[1] + "," + headernew[2] + ","+ headernew[3] + ","+ headernew[4] + ","+ headernew[5]+","+ headernew[6] + "\n");
			writer.writeNext(headernew);
			previousLine = nextLine;
		}
		reader.close();
		writer.close();
		bw.close();
	}
}