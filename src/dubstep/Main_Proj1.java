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
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Scanner;
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
import java.io.Reader;


public class Main_Proj1{
	static String[] rowData = null;
	//public enum columndDataTypess  {String,varchar,Char,Int,decimal,date}; 
	public static BufferedReader br = null;
	
	static Reader in = null;
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


	public static void getSelectedColumns(String tableName, Expression whereExpression) throws IOException
	{
		try{			
			//Table table = (Table) plain.getFromItem();
			//String tableName = table.getName();
			String csvFile_local_copy = csvFile+tableName+".csv";
			br = new BufferedReader(new FileReader(String.format(csvFile_local_copy)));
			StringBuilder sb = new StringBuilder();
			EvalLib e = new EvalLib(tableName);
			PrimitiveValue pv = null;
			double[] aggrMap = new double[10];
			String[] aggrStrMap = new String[10];
			int[] aggrDenomMap = new int[10];

			List<SelectItem> selectItems = plain.getSelectItems();
			ArrayList <Expression> selectlist = new ArrayList<Expression>();
			boolean whereclauseabsent = (plain.getWhere()==null)?true:false;
			boolean isStarPresent = false;
			boolean is_aggregate=false;
			//System.out.println("is_aggregate = " + is_aggregate);
			PrimitiveValue Max = null;
			PrimitiveValue Min = null;
			int countAll =0;
			int countNonNull =0;
			double total=0;
			double sum=0;
			PrimitiveValue result=null;
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
				int index=0;
				String temp = "";
//				in = new FileReader(csvFile_local_copy);
//				csvParser = new CSVParser(in, CSVFormat.EXCEL.withIgnoreHeaderCase().withHeader(columnNameToIndexMapping.get(tableName).keySet().toString()).withDelimiter('|'));					
//				Iterator<CSVRecord> iter = csvParser.iterator();
				//while(iter.hasNext())
				while((line=br.readLine())!=null)
				{
					//record = iter.next();
					//System.out.println("Debug: "+line);
					rowData = line.split("\\|",-1);
					if(whereclauseabsent || e.eval(whereExpression).toBool())
					{
						if (is_aggregate == false) // Either Select Can have aggregate, with columns OR *
						{
							for(int i=0;i<selectItems.size()-1;i++)
							{
								result = e.eval(((SelectExpressionItem)selectItems.get(i)).getExpression());
								sb.append(result.toRawString().concat("|"));
							}
							result = e.eval(((SelectExpressionItem)selectItems.get(selectItems.size()-1)).getExpression());
							sb.append(result.toRawString()+"\n");
						}
						else
						{

							for(int i =0; i<selectlist.size();i++)
							{
								Function item = null;
								Expression operand = null;
								if(selectlist.get(i) instanceof Function){
									item = (Function) selectlist.get(i);
									switch (item.getName().toLowerCase()){
									case "avg":								

										operand = (Expression) item.getParameters().getExpressions().get(0);
										index =columnNameToIndexMapping.get(tableName).get(operand.toString());
										//temp =record.get(index);
										if(rowData[index].length() != 0)
										{
											result = e.eval(operand);
											if(result!=null)
											{
												aggrMap[i]+=result.toDouble();
												aggrDenomMap[i]+=1;
											}
										}
										break;
									case "sum":
										operand = (Expression) item.getParameters().getExpressions().get(0);
										result = e.eval(operand);

										if(result!=null)
										{
											aggrMap[i]+=result.toDouble();
										}
										break;
									case "count":
										if(item.toString().toLowerCase().contains("count(*)"))
										{
											aggrMap[i]+=1;
										}
										else{
											operand = (Expression) item.getParameters().getExpressions().get(0);
											index =columnNameToIndexMapping.get(tableName).get(operand.toString());
											temp =rowData[index];//record.get(index);//(rowData[index]);
											if(temp.trim().length() != 0)
											{
												//result = e.eval(operand);
												if(e.eval(operand)!=null)
												{
													aggrMap[i]+=1;
												}
											}
										}
										break;
									case "min":
										operand = (Expression) item.getParameters().getExpressions().get(0);
										index =columnNameToIndexMapping.get(tableName).get(operand.toString());
										temp =rowData[index];//record.get(index);//(rowData[index]);
										if(temp.trim().length() != 0)
										{
											result = e.eval(operand);
											if(result.toDouble() < aggrMap[i])
											{
												aggrMap[i] = result.toDouble();
											}
										}
										break;
									case "max":
										operand = (Expression) item.getParameters().getExpressions().get(0);
										index =columnNameToIndexMapping.get(tableName).get(operand.toString());
										temp =rowData[index];//record.get(index);//(rowData[index]);
										if(temp.trim().length() != 0)
										{
											result = e.eval(operand);
											if(result.toDouble() > aggrMap[i])
											{
												aggrMap[i]=result.toDouble();
											}
										}
										break;
									default:
										break;

									}

								}else{// If the Column is not an aggregate column then simply get the value
									operand = selectlist.get(i);
									if(aggrStrMap[i]==null){
										aggrStrMap[i] =  e.eval(operand).toRawString();
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
						switch(item.getName()){
						case "SUM":
						case "MIN":
						case "MAX":
							sb.append((result instanceof LongValue)?(((long)aggrMap[i])+"|"):aggrMap[i]+"|");
							break;
						case "count":
						case "COUNT":
							sb.append((long)aggrMap[i]+"|");
							break;
						case "AVG":
							sb.append((aggrMap[i]/aggrDenomMap[i])+"|");
							break;
						default: break;
						}
					}else{// if its a Simple String Column or Simple Number Column
						sb.append(aggrStrMap[i]+"|");
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
				//return new StringValue(record.get(index));
			case "int": 
				return new LongValue(rowData[index]);
				//return new LongValue(record.get(index));
			case "decimal":
				return new DoubleValue(rowData[index]);
				//return new DoubleValue(record.get(index));
			case "date":
				return new DateValue(rowData[index]);
				//return new DateValue(record.get(index));
			default:
				return new StringValue(rowData[index]);
				//return new StringValue(record.get(index));
			}

		}
	}

}