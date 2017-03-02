package dubstep;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Scanner;
import java.util.Stack;

import net.sf.jsqlparser.eval.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExpressionVisitorBase;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;




public class Main{
	static class EvalLib extends Eval{
		String tableName = "";

		public EvalLib(String tableName){
			this.tableName = tableName;
		}
		@Override
		public PrimitiveValue eval(Column col) throws SQLException {
			
			
				int index =columnNameToIndexMapping.get(tableName).get(col.getColumnName());
			if(columnDataTypes.get(tableName).get(index).equals("String"))
			{
				return new StringValue(rowData[index].trim());
			}
			else if(columnDataTypes.get(tableName).get(index).equals("int"))
			{
				return new LongValue(rowData[index].trim());
			}
			else if(columnDataTypes.get(tableName).get(index).equals("varchar"))
			{
				return new StringValue(rowData[index].trim());
			}
			else if(columnDataTypes.get(tableName).get(index).equals("char"))
			{
				return new StringValue(rowData[index].trim());
			}
			else if(columnDataTypes.get(tableName).get(index).equals("decimal"))
			{
				return new DoubleValue(rowData[index].trim());
			}
			else if(columnDataTypes.get(tableName).get(index).equals("date"))
			{
				return new DateValue(rowData[index].trim());
			}
			return null;

		}

	}
	static String[] rowData = null;
	//public enum columndDataTypess  {String,varchar,Char,Int,decimal,date}; 
	public static BufferedReader br = null;
	public static String csvFile = "src\\dubstep\\data\\";
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

	public static void main(String[] args) throws Exception
	{


		//create table R(A int, B String, C String, D int ); select A,B,C,D from R
		//create table R(A int, B String, C String, D int ); select A,B from R where A=1 and B=1;select * from R
		//create table R(A int, B String, C String, D int ); select A,B from R where (A=1 and B=2) OR (B=1 AND D =9)
		//create table R(A int, B String, C String, D int ); select A,B from R where (A=1 and B=2) OR (C>4 AND B=1 AND D =9)
		//create table R(A int, B String, C String, D int ); select A,B from R where (A=1 and B=2) OR (C>4 AND B=1) AND (B>2 OR D =9)
		
		System.out.print("$> ");
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

				//System.out.println("statement = "+statement);
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
			getSelectiveColumnsAsPerSelectStatement(tableName, plain.getWhere());

		}

		else {
			throw new Exception("I can't understand body instanceof PlainSelect "+statement);
		}
		/** Do something with the select operator **/

	}


	public static void getSelectiveColumnsAsPerSelectStatement(String tableName, Expression whereExpression) throws IOException
	{
		try{
			//Table table = (Table) plain.getFromItem();
			//String tableName = table.getName();
			String csvFile_local_copy = csvFile+tableName+".csv";
			br = new BufferedReader(new FileReader(String.format(csvFile_local_copy)));
			StringBuilder sb = new StringBuilder();
			EvalLib e = new EvalLib(tableName);
			PrimitiveValue pv = null;

			/*
			import org.apache.commons.csv.CSVFormat;
			import org.apache.commons.csv.CSVParser;
			import org.apache.commons.csv.CSVRecord;

			Reader in = new FileReader(file);
						CSVParser parser = new CSVParser(in, CSVFormat.EXCEL.withHeader(tblschema.getHeaders()).withDelimiter('|'));
			*/
			List<SelectItem> selectclauses = plain.getSelectItems();
			ArrayList <Function> selectlist = new ArrayList<Function>();
			
			for(SelectItem selectclause: selectclauses)
			{
				Expression expression = ((SelectExpressionItem)selectclause).getExpression();
				if(expression instanceof Function)
				{
						Function exp =(Function)expression;
						selectlist.add(exp);				

				}
			}
			
			for(Function item : selectlist)
			{
				
				System.out.println(item);
				if(item.getName().equals("SUM"))
				{
					Expression operand1 = (Expression) item.getParameters().getExpressions().get(0);
					Expression operand2 = (Expression) item.getParameters().getExpressions().get(1);
					//PrimitiveValue temp = e.eval(item.getParameters().getExpressions().get(0));
				}
				else if(item.getName().equals("AVG"))
				{
					System.out.println(item);

				}
				else if(item.getName().equals("COUNT"))
				{
					System.out.println(item);
					Expression operand = (Expression) item.getParameters().getExpressions().get(0);
					//PrimitiveValue temp = e.eval(item.getParameters().getExpressions().get(0));
				}
				else if(item.getName().equals("MIN"))
				{
					System.out.println(item);
					Expression operand = (Expression) item.getParameters().getExpressions().get(0);
					//PrimitiveValue temp = e.eval(item.getParameters().getExpressions().get(0));
				}
				else if(item.getName().equals("MAX"))
				{
					System.out.println(item);
					Expression operand = (Expression) item.getParameters().getExpressions().get(0);
					//PrimitiveValue temp = e.eval(item.getParameters().getExpressions().get(0));
				}
				else if(item.getName().equals("MAX"))
				{
					System.out.println(item);
					Expression operand = (Expression) item.getParameters().getExpressions().get(0);
					//PrimitiveValue temp = e.eval(item.getParameters().getExpressions().get(0));
				}
			}
			
			while ((line = br.readLine()) != null) {
				//System.out.println("Debug: "+line);
				String Line1 = line.replace("|", "| ");
				rowData = Line1.split("\\|");
				if(plain.getWhere()!=null){
					pv = e.eval(whereExpression);
					if(pv.toBool()){
						for(int i=0;i<selectclauses.size()-1;i++)
						{
							sb.append(rowData[columnNameToIndexMapping.get(tableName).get(selectclauses.get(i).toString())].trim()+"|");
						}

						sb.append(rowData[columnNameToIndexMapping.get(tableName).get(selectclauses.get(selectclauses.size()-1).toString())].trim()+"\n");
					}
				}
				else{
					for(int i=0;i<selectclauses.size()-1;i++)
					{
						sb.append(rowData[columnNameToIndexMapping.get(tableName).get(selectclauses.get(i).toString())].trim()+"|");
					}
					
					//System.out.println("x  :"+rowData.length);

					sb.append(rowData[columnNameToIndexMapping.get(tableName).get(selectclauses.get(selectclauses.size()-1).toString())].trim()+"\n");
					
				}
				
				
			}
			System.out.print(sb.toString());
		
		}
		catch(SQLException e){
			System.out.println(e.getMessage());

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

}

class WhereCondition{
	String var1 = "";
	String var2 = "";
	String operator="";
	public WhereCondition(String var1,String var2,String operator){
		this.var1 = var1;
		this.var2 = var2;
		this.operator=operator;
	}
}