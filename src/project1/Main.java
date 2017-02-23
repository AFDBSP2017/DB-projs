
package project1;
import java.io.StringReader;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Scanner;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.expression.Expression;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;



public class Main{

	
	//public enum columndDataTypess  {String,varchar,Char,Int,decimal,date}; 
    public static BufferedReader br = null;
    //String currentDir = System.getProperty("user.dir"); 
    //System.out.println(currentDir);
    public static String csvFile = "src\\project1\\data\\";
    public static String line = "";
    public static Statement statement;
    public static Scanner scan;
    public static CCJSqlParser parser;
    public static PlainSelect plain;
    public static int columnIndexesToFetchInSelectStatement[];
    public static ArrayList <String> columnDataTypes = new ArrayList <String>();
    public static Map<String,Integer> columnNameToIndexMap = null;
    public static Select select;
    public static SelectBody body;
    
    public static void main(String[] args) throws Exception
    {

    	//create table R(A int, B String, C String, D int ); select A,B,C,D from R
    	//create table R(A int, B String, C String, D int ); select A,B from R where A=1 and B=1;select * from R
    	readQueries();
    	parseQueries();

    }
    
    
    
    public static void parseQueries() throws Exception
    {

        while(statement != null)
        {

            if(statement instanceof CreateTable)
            {
            	getColumnDataTypesAndMapColumnNameToIndex();
            }
            else if(statement instanceof Select)
            {
            	System.out.println("statement = "+statement);
            	parseSelectStatement();     
            	initialiseDataStructures(); //to process next command

            } 
            else 
            {
                throw new Exception("I can't understand statement instanceof Select"+statement);
            }
            statement = parser.Statement();
        }
    }
    
    

    public static void initialiseDataStructures()
    {
        columnIndexesToFetchInSelectStatement = null;
        //columnDataTypes = null;  //create table is one command
        //columnNameToIndexMap = null;
    	 csvFile = "src\\project1\\data\\";
    }
    
    
    public static void parseSelectStatement() throws Exception
    {

//    	Iterator it = s.listIterator();
//    	while(it.hasNext()){
//    		System.out.println(it.next());
//    	}
        select = (Select)statement;
        body = select.getSelectBody();
        

        if(body instanceof PlainSelect){
        	
        	findcolumnsToFetchInSelectStatement();
        	getSelectiveColumnsAsPerSelectStatement();

        	if (plain.getWhere() != null) 
        	{
        		System.out.println(plain.getWhere());
        	}
        	
//            BinaryExpression ex   =  (BinaryExpression)plain.getWhere();
//            System.out.println(
//           ex.getLeftExpression()+" , "+
//           ex.getRightExpression()+" , "+
//           ex.getStringExpression());
            //Expression ex = ((PlainSelect) body).getWhere().accept();;
        }

        else {
            throw new Exception("I can't understand body instanceof PlainSelect "+statement);
        }
        /** Do something with the select operator **/
    
    }
    
    
    public static void getSelectiveColumnsAsPerSelectStatement() throws IOException
    {

    	//Get Table
    	Table table = (Table) plain.getFromItem();
        String tableName = table.getName();
        csvFile = csvFile+tableName+".csv";
        //System.out.println(csvFile);
        br = new BufferedReader(new FileReader(String.format(csvFile)));
        
        
        while ((line = br.readLine()) != null) {

            // use | as separator
            String[] ColumnsInSelectStatement = line.split("\\|");

            for(int i=0;i<columnIndexesToFetchInSelectStatement.length-1;i++)
            {
                System.out.print(ColumnsInSelectStatement[columnIndexesToFetchInSelectStatement[i]]+"|");
            }
            System.out.print(ColumnsInSelectStatement[columnIndexesToFetchInSelectStatement[columnIndexesToFetchInSelectStatement.length-1]]+"\n");
        }
    }
    
    
    /* get all columns to fetch as per Select Statement*/
    public static void findcolumnsToFetchInSelectStatement()
    {
        plain = (PlainSelect)body;
        List<SelectItem> si =  plain.getSelectItems();
        ListIterator<SelectItem> it = si.listIterator();
        
        String str = si.toString();
        if(str.contains("*"))
        {
        	//System.out.println("Contains *" + si);
        	columnIndexesToFetchInSelectStatement = new int[columnNameToIndexMap.size()] ;
        }
        else
        {
        	columnIndexesToFetchInSelectStatement = new int[si.size()] ;
        }
        System.out.println("Size  = " +columnIndexesToFetchInSelectStatement.length);
        int i=0;
        
        //column names in select statement
        while(it.hasNext()){
        	String col_name = (String) it.next().toString();
        	System.out.println("Column to fetch in Select Query = " +col_name);
        	
        	if(col_name.equals("*"))  //fetch all the columns
        	{
        		//put all column names in fetch list
        		for (Map.Entry<String, Integer> entry : columnNameToIndexMap.entrySet()) {
        		    //String key = entry.getKey();
        		    Integer col_index = entry.getValue();
        		   // System.out.println("value = " +value);
        		    columnIndexesToFetchInSelectStatement[i]=col_index; //save indexes of all columns to fetch
                	i++;
        		}
        		
        		return; //  "*" cannot be with any other column name
        	}
        	else
        	{
	        	//System.out.println("Corresponding Column Index = " + columnNameToIndexMap.get(col_name));
	        	columnIndexesToFetchInSelectStatement[i]=columnNameToIndexMap.get(col_name); //save indexes of all columns to fetch
	        	i++;
        	}
        }
    }
    
    
    public static void getColumnDataTypesAndMapColumnNameToIndex()
    {

    	CreateTable create = (CreateTable)statement;
    	columnNameToIndexMap = new HashMap<String,Integer>();
    	List<ColumnDefinition> si = create.getColumnDefinitions();
    	ListIterator<ColumnDefinition> it = si.listIterator();
    	
    	int i=0;
        while(it.hasNext())
        {
        	//System.out.println(it.next());
        	
        	ColumnDefinition cd = it.next();
        	columnDataTypes.add(cd.getColDataType().toString());
        	System.out.println("type = "+ columnDataTypes.get(i));
        	columnNameToIndexMap.put(cd.getColumnName() ,i++);
        }
    }
    
    
    public static void readQueries() throws ParseException
    {
        scan = new Scanner(System.in);
        String temp = scan.nextLine();
        scan.close();
        StringReader input = new StringReader(temp);
        parser = new CCJSqlParser(input);
        statement = parser.Statement();    	
    }
    


}