//create table R(A int, B String, C String, D int );Select SUM(A),B, SUM(D) From R
//create table R(A int, B String, C String, D int );Select SUM(A),Count(*), SUM(D),MIN(A),MAX(D),AVG(D),Count(C) From R
//create table R(A int, B String, C String, D int );Select SUM(A), SUM(D) From R;
//create table R(A int, B String, C String, D int ); select A,B,C,D from R
//create table R(A int, B String, C String, D int ); select A,B from R where A=1 and B=1;select * from R
//create table R(A int, B String, C String, D int ); select A,B from R where (A=1 and B=2) OR (B=1 AND D =9)
//create table R(A int, B String, C String, D int ); select A,B from R where (A=1 and B=2) OR (C>4 AND B=1 AND D =9)
//create table R(A int, B String, C String, D int ); select A,B from R where (A=1 and B=2) OR (C>4 AND B=1) AND (B>2 OR D =9)

package dubstep;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.eval.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;


public class Main{
	static String[] rowData = null;
	//public enum columndDataTypess  {String,varchar,Char,Int,decimal,date}; 
	public static BufferedReader br = null;
	//

	public static String csvFile = "src\\dubstep\\data\\";
	//public static String csvFile = "data/";
	public static String line = "";
	public static Statement statement;
	public static Scanner scan;
	public static CCJSqlParser parser;
	public static PlainSelect plain;
	public static Map<String,ArrayList <String>> columnDataTypes = new HashMap<String,ArrayList <String>>();
	public static Map<String,Map<String,Integer>> columnNameToIndexMapping = new HashMap<String,Map<String,Integer>>();
	public static Select select;
	public static SelectBody body;
	public static Map<String, PrimitiveValue> rowMap= null;
	public static StringTokenizer  st = null;



	public static void main(String[] args) throws Exception
	{


		System.out.print("$>");
		scan = new Scanner(System.in);
		String temp;
		while((temp = scan.nextLine()) != null)
		{

			readQueries(temp);

			parseQueries();
			System.out.print("$>");
		}
		scan.close();
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

				parseSelectStatement();     
			} 
			else 
			{
				throw new Exception("I can't understand statement instanceof Select"+statement);
			}
			statement = parser.Statement();
		}
	}



	public static void parseSelectStatement() throws Exception
	{

		select = (Select)statement;
		body = select.getSelectBody();


		if(body instanceof PlainSelect){

			plain = (PlainSelect)body;
			Table table = (Table) plain.getFromItem();
			String tableName = table.getName();
			getSelectedColumns(tableName, plain.getWhere());

		}

		else {
			throw new Exception("I can't understand body instanceof PlainSelect "+statement);
		}
		/** Do something with the select operator **/

	}
	static int countAll =0;
	static int countNonNull =0;
	static Map<Integer,Double> aggrMap = new HashMap<Integer, Double>();
	static Map<Integer,String> aggrStrMap = new HashMap<Integer, String>();
	static Map<Integer,Integer> aggrDenomMap = new HashMap<Integer, Integer>();
	static Function item = null;
	static Expression operand = null;
	static String tableName = "";
	static int index=0;
	static PrimitiveValue result=null;
	static String temp = "";
	static EvalLib e=null;
	public static void getSelectedColumns(String tN, Expression whereExpression) throws IOException
	{
		try{			
			tableName = tN;
			//Table table = (Table) plain.getFromItem();
			//String tableName = table.getName();
			String csvFile_local_copy = csvFile+tableName+".csv";
			br = new BufferedReader(new FileReader(String.format(csvFile_local_copy)));
			StringBuilder sb = new StringBuilder();
			e = new EvalLib(tableName);
			PrimitiveValue pv = null;
			
			List<SelectItem> selectItems = plain.getSelectItems();
			ArrayList <Expression> selectlist = new ArrayList<Expression>();
			boolean whereclauseabsent = (plain.getWhere()==null)?true:false;
			boolean isStarPresent = false;
			boolean is_aggregate=false;
			//System.out.println("is_aggregate = " + is_aggregate);
			PrimitiveValue Max = null;
			PrimitiveValue Min = null;
			
			double total=0;
			double sum=0;
			
			//PrimitiveValue sum ;
			int row_count=0;
			double average=0;

			for(SelectItem select: selectItems)
			{
				//System.out.println(select);

				if(select.toString().equals("*")){ isStarPresent = true; break; }
				Expression expression = ((SelectExpressionItem)select).getExpression();
				if(expression instanceof Function)
				{
					is_aggregate = true;
					selectlist.add((Function) expression);
				}
				else{
					selectlist.add(expression);
				}
			}
			if(isStarPresent){ // If a star is present then read the file at once to avoid i/o costs

				sb.append(String
						.join(
								System.getProperty("line.separator")
								,Files.readAllLines(Paths.get(csvFile_local_copy))
								)
						);

			}
			else{
				//System.out.println("totalCount:"+br.lines().count());
				
				//List<String> lineList = Files.readAllLines(Paths.get(csvFile_local_copy));
				//Pattern p = Pattern.compile("\\|");
				while ((line = br.readLine()) != null) 
				//for(String line : lineList)
				{
					//System.out.println("Debug: "+line);
					
					doSplit();//getTokens(line.replaceAll("\\|", "\\| "),"\\|",true);
					//st = new StringTokenizer(line.replaceAll("\\|", "\\| "),"\\|");
					if(whereclauseabsent || e.eval(whereExpression).toBool())
					{
						if (is_aggregate == false) // Either Select Can have aggregate, with columns OR *
						{
							for(int i=0;i<selectItems.size()-1;i++)
							{
								result = e.eval(((SelectExpressionItem)selectItems.get(i)).getExpression());
								sb.append(result.toRawString()).append("|");
							}
							result = e.eval(((SelectExpressionItem)selectItems.get(selectItems.size()-1)).getExpression());
							sb.append(result.toRawString()).append(System.getProperty("line.separator"));
						}
						else
						{

							for(int i =0; i<selectlist.size();i++)
							{
								
								if(selectlist.get(i) instanceof Function){
									item = (Function) selectlist.get(i);
									if(aggrMap.get(i)==null){
										aggrMap.put(i,0.0);
									}

									switch (item.getName().toLowerCase()){
									case "avg":
										doAvg(i);
										break;
									case "sum":
										doSum(i);
										break;
									case "count":
										if(item.toString().toLowerCase().contains("count(*)"))
										{	
											doCountStar(i);
										}
										else{
											doCount(i);
										}
										break;
									case "min":
										doMin(i);
										break;
									case "max":
										doMax(i);
										break;
									default:
										break;

									}
								}else{// If the Column is not an aggregate column then simply get the value
									operand = selectlist.get(i);
									if(aggrStrMap.get(i)==null){
										aggrStrMap.put(i, e.eval(operand).toRawString());
									}
								}
							}
						}
					}
				}
			}

			if(is_aggregate==true)
			{
				for(int i =0; i<selectlist.size();i++)
				{
					Function item = null;

					if(selectlist.get(i) instanceof Function){
						item = (Function) selectlist.get(i);
						if(item.getName().equalsIgnoreCase("SUM")
								||item.getName().equalsIgnoreCase("Min")
								||item.getName().equalsIgnoreCase("COUNT")
								||item.toString().toLowerCase().contains("count(*)")
								||item.getName().equalsIgnoreCase("Max"))
						{
							sb.append((result instanceof LongValue)?(aggrMap.get(i).longValue())+"|":aggrMap.get(i)+"|");
						}
						else if(item.getName().equalsIgnoreCase("AVG")){
							sb.append((aggrMap.get(i)/aggrDenomMap.get(i))).append("|");
						}
					}else{// if its a Simple String Column or Simple Number Column
						sb.append(aggrStrMap.get(i)).append("|");
					}
				}
			}
			if(sb.length() >=2)
			{
				sb.setLength(sb.length() - 1);
				sb.append("\n");
			}
			System.out.print(sb);
		}
		catch(SQLException e){
			System.out.println(e.getMessage());

		}
	}

	public static void doSplit(){
		rowData =  line.split("\\|",-1);
	}
	public static void doAvg(int i) throws SQLException {
		aggrDenomMap.put(i, 0);
		operand = (Expression) item.getParameters().getExpressions().get(0);
		index =columnNameToIndexMapping.get(tableName).get(operand.toString());
		temp = ( rowData[index]);
		if(temp.trim().length() != 0)
		{
			result = e.eval(operand);
			if(result!=null)
			{
				aggrMap.put(i, aggrMap.get(i)+result.toDouble());
				aggrDenomMap.put(i, (aggrDenomMap.get(i)+1));
			}
		}
	}
	public static void doMin(int i) throws SQLException{
		operand = (Expression) item.getParameters().getExpressions().get(0);
		index =columnNameToIndexMapping.get(tableName).get(operand.toString());
		temp = (rowData[index]);
		if(temp.trim().length() != 0)
		{
			result = e.eval(operand);
			if(result.toDouble() < aggrMap.get(i))
			{
				aggrMap.put(i,result.toDouble());
			}
		}
	}
	public static void doMax(int i) throws SQLException{
		operand = (Expression) item.getParameters().getExpressions().get(0);
		index =columnNameToIndexMapping.get(tableName).get(operand.toString());
		temp = (rowData[index]);
		if(temp.trim().length() != 0)
		{
			result = e.eval(operand);
			if(result.toDouble() > aggrMap.get(i))
			{
				aggrMap.put(i,result.toDouble());
			}
		}
	}
	public static void doCount(int i) throws SQLException{
		operand = (Expression) item.getParameters().getExpressions().get(0);
		index =columnNameToIndexMapping.get(tableName).get(operand.toString());
		temp =  (rowData[index]);
		if(temp.trim().length() != 0)
		{
			//result = e.eval(operand);
			if(e.eval(operand)!=null)
			{
				aggrMap.put(i,(aggrMap.get(i)+1));
			}
		}
	}
	public static void doCountStar(int i){
		countAll++;
		aggrMap.put(i,(double) countAll);
	}
	public static void doSum(int i) throws SQLException{
		operand = (Expression) item.getParameters().getExpressions().get(0);
		result = e.eval(operand);

		if(result!=null)
		{
			aggrMap.put(i, aggrMap.get(i)+result.toDouble());
		}
	}
	public static void getColumnDataTypesAndMapColumnNameToIndex() throws SQLException
	{

		CreateTable create = (CreateTable)statement;
		String tableName = create.getTable().getName();
		//System.out.println(tableName);
		Map<String,Integer> columnNameToIndexMap = new HashMap<String,Integer>();
		List<ColumnDefinition> si = create.getColumnDefinitions();
		ListIterator<ColumnDefinition> it = si.listIterator();
		ArrayList <String> dataTypes = new ArrayList <String>();

		int i=0;
		while(it.hasNext())
		{
			ColumnDefinition cd = it.next();
			dataTypes.add(cd.getColDataType().toString());
			//System.out.println("type = "+ dataTypes.get(i));
			columnNameToIndexMap.put(cd.getColumnName() ,i++);
		}
		columnDataTypes.put(tableName, dataTypes);
		columnNameToIndexMapping.put(tableName,columnNameToIndexMap);
	}


	public static void readQueries(String temp) throws ParseException
	{

		StringReader input = new StringReader(temp);
		parser = new CCJSqlParser(input);
		statement = parser.Statement();  
	}

	static class EvalLib extends Eval{
		String tableName = "";
		public EvalLib(String tableName){
			this.tableName = tableName;
		}
		@Override
		public PrimitiveValue eval(Column col) throws SQLException {
			int index =columnNameToIndexMapping.get(tableName).get(col.getColumnName());
			switch(columnDataTypes.get(tableName).get(index))
			{
			case "String":
			case "varchar":
			case "char":
				return new StringValue(rowData[index]);
			case "int":
				return new LongValue(rowData[index]);
			case "decimal":
				return new DoubleValue(rowData[index]);
			case "date":
				return new DateValue(rowData[index]);
			default:
				return new StringValue(rowData[index]);
			}

		}
	}

}