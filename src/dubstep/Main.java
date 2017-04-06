package dubstep;

import java.io.BufferedReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Scanner;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;

public class Main {

	public static Scanner scan;
	static String[] rowData = null; 
	public static BufferedReader br = null;
	
	static Reader in = null;
	public static String csvFile = "src\\dubstep\\data\\";
	//public static String csvFile = "data/";
	public static String line = "";
	public static Statement statement;
	public static CCJSqlParser parser;
	
	public static void readQueries(String temp) throws ParseException
	{

		StringReader input = new StringReader(temp);
		parser = new CCJSqlParser(input);
		statement = parser.Statement();  
	}

	public static void main(String[] args) throws ParseException {
		System.out.print("$>");
		scan = new Scanner(System.in);
		String temp;
		while((temp = scan.nextLine()) != null)
		{

			readQueries(temp);

			//parseQueries();
			System.out.print("$>");
		}
		scan.close();

	}

}
