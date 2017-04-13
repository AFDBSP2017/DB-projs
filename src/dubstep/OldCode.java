//create table R(A int, B String, C String, D int );Select SUM(A),B, SUM(D) From R
//create table R(A int, B String, C String, D int );Select SUM(A),Count(*), SUM(D),MIN(A),MAX(D),AVG(D),Count(C) From R
//create table R(A int, B String, C String, D int );Select SUM(A), SUM(D) From R;
//create table R(A int, B String, C String, D int ); select A,B,C,D from R
//create table R(A int, B String, C String, D int ); select A,B from R where A=1 and B=1;select * from R
//create table R(A int, B String, C String, D int ); select A,B from R where (A=1 and B=2) OR (B=1 AND D =9)
//create table R(A int, B String, C String, D int ); select A,B from R where (A=1 and B=2) OR (C>4 AND B=1 AND D =9)
//create table R(A int, B String, C String, D int ); select A,B from R where (A=1 and B=2) OR (C>4 AND B=1) AND (B>2 OR D =9)

package dubstep;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Scanner;
import java.util.Date.*;
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


public class OldCode{


	static String[] rowData = null;
	
	//aggregateHistory is HashMap <Table ,<Aggregate Value of each column in this table>> 
	public static Map<String,Map<String,Map<String,Double>>> aggregateHistory=new HashMap<String,Map<String,Map<String,Double>>> ();
	//public enum columndDataTypess  {String,varchar,Char,Int,decimal,date}; 
	public static BufferedReader br = null;
	//

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
	public static String[] columnDataTypeArray = null;
	public static String[] columnIndexArray = null;
	public static List<String> columnIndexList = null;

	public static void main(String[] args) throws Exception
	{


		System.out.print("$>");
		scan = new Scanner(System.in);
		String temp;
		while((temp = scan.nextLine()) != null)
		{
			
			//Read Lines from console which contains multiple CreateTable/Selection Queries
			readQueries(temp);

			//Call function to Start Parsing each Statemetn one by One
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


	/*******************************************
	1> Read Select Statement one by one.
	2> Check if  This is Plain or Aggrregate query
	3> If its Plain query, fetch the Select value using Evallib
	4> If its Aggregate Query, then perform the operation as per aggregate function
	*/
	public static void getSelectedColumns(String tableName, Expression whereExpression) throws IOException
	{
		try{
			Date d1 = new Date();
			long t1 = d1.getTime();
			//Initialize Variable
			columnIndexArray = new String[columnNameToIndexMapping.get(tableName).keySet().size()];
			columnDataTypeArray = new String[columnDataTypes.get(tableName).size()];
			columnIndexList = Arrays.asList(columnIndexArray);
			Map<String, Integer> q = columnNameToIndexMapping.get(tableName);//.keySet()..toArray(columnIndexArray);
			columnDataTypes.get(tableName).toArray(columnDataTypeArray);
			for(String k: q.keySet()){
				columnIndexList.set(q.get(k),k);
			}
			String csvFile_local_copy = csvFile+tableName+".csv";
			br = new BufferedReader(new FileReader(String.format(csvFile_local_copy)));
			StringBuilder sb = new StringBuilder();
			EvalLib e = new EvalLib(tableName);
			PrimitiveValue pv = null;
			double[] aggrMap = new double[10];
			String[] aggrStrMap = new String[10];
			int[] aggrDenomMap = new int[10];
			ArrayList <Expression> originalSelectList = new ArrayList<Expression>();
			List<SelectItem> selectItems = plain.getSelectItems();
			ArrayList <Expression> selectlist = new ArrayList<Expression>();
			boolean whereclauseabsent = (plain.getWhere()==null)?true:false;
			boolean isStarPresent = false;
			boolean is_aggregate=false;
			PrimitiveValue result=null;
			
			//Extract individual Select Statements in selectlist
			for(SelectItem select: selectItems)
			{
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
				originalSelectList.add(((SelectExpressionItem)select).getExpression());
			}


			
			//History of Aggregate Function is saved
			//If Already Aggregate Computation exists(from last query which was executed), delete this entry from to-Compute list
			//Also, delete duplicate Selects in a single query.			
			ArrayList<Integer> indicesToRemove = new ArrayList<Integer>();
			if(is_aggregate){


				for(int i =0; i<originalSelectList.size();i++)
				{
					String columname ="";
					if(originalSelectList.get(i).toString().contains("(*)"))
					{
						columname = "COUNT_A";
					}
					else
					{
						columname = ((Function) originalSelectList.get(i)).getParameters().getExpressions().get(0).toString();
					}

					Function item = null;
					item = (Function) originalSelectList.get(i);
					String aggregateFunction = item.getName(); 

					if(aggregateHistory.get(tableName)==null)
					{
						//aggregateHistory.get(tableName).putIfAbsent(columname, null);
						Map<String,Double> agg= new HashMap<String,Double>();
						agg.put("SUM",null);
						agg.put("MAX",null);
						agg.put("MIN",null);
						agg.put("AVG",null);
						agg.put("COUNT",null);
						Map<String,Map<String,Double>> col = new HashMap<String,Map<String,Double>>();
						col.put(columname, agg);
						aggregateHistory.put(tableName, col);

					}
					else if(aggregateHistory.get(tableName).get(columname)==null)
					{

						Map<String,Double> agg= new HashMap<String,Double>();
						agg.put("SUM",null);
						agg.put("MAX",null);
						agg.put("MIN",null);
						agg.put("AVG",null);
						agg.put("COUNT",null);

						aggregateHistory.get(tableName).put(columname, agg);
					}


					else if(aggregateHistory.get(tableName).get(columname).get(aggregateFunction)!=null)
					{
						indicesToRemove.add(i);
					}
				}


				if(aggregateHistory.get(tableName).get("COUNT_A")==null)
				{
					Map<String,Double> agg= new HashMap<String,Double>();
					agg.put("COUNT",null);					
					aggregateHistory.get(tableName).put("COUNT_A", agg);
				}


				Collections.sort(indicesToRemove, Collections.reverseOrder());
				for (int i : indicesToRemove)
				{
					selectlist.remove(i);
				}



				ArrayList<String> avoidDuplicates = new ArrayList<String> ();
				ArrayList<Expression> l = new ArrayList<Expression>();

				//Remove Duplicates
				for(int idx = 0 ; idx<selectlist.size();idx++)
				{
					if(avoidDuplicates.indexOf(selectlist.get(idx).toString())==-1)
					{
						avoidDuplicates.add(selectlist.get(idx).toString());
						l.add(selectlist.get(idx));
					}
				}
				
				if(l.size()>1){
					for(int idx = 0 ; idx<l.size();idx++)
					{
						if(l.get(idx).toString().contains("*")){
							l.remove(idx);
							break;
						}
					}
				}
				selectlist = l;
			}
			double count_All=0;
			Function  item= null;
			Expression operand = null;
			if(isStarPresent){ // If a star is present then read the file at once to avoid i/o costs
				sb.append(String
						.join(
								System.getProperty("line.separator")
								,Files.readAllLines(Paths.get(csvFile_local_copy))
								)
						);

			}
			else{


				String temp = "";

				if((is_aggregate==true) && selectlist.size()==0)
				{
						//If All the needed information already exists in Memory, Dont do any Computation 
				}
				else{
					while((line=br.readLine())!=null)     //Read all the lines 1-by-1
					{
						count_All++;
						rowData = line.split("\\|",-1);
						if(whereclauseabsent || e.eval(whereExpression).toBool())
						{
							if (is_aggregate == false) //If there is no Aggregate Statement in Query
							{
								for(int i=0;i<selectItems.size()-1;i++)
								{
									result = e.eval(((SelectExpressionItem)selectItems.get(i)).getExpression());
									sb.append(result.toRawString().concat("|"));
								}
								result = e.eval(((SelectExpressionItem)selectItems.get(selectItems.size()-1)).getExpression());
								sb.append(result.toRawString()+"\n");
							}
							else    //If its aggregate Query
							{
								for(int i =0; i<selectlist.size();i++)
								{									
									if(selectlist.get(i) instanceof Function )
									{
										item = (Function) selectlist.get(i);
										switch (item.getName().charAt(2)){
										case 'X':    //If Max
											operand = (Expression) item.getParameters().getExpressions().get(0);
											result = e.eval(operand);
											if(aggrMap[i] < result.toDouble())
											{
												aggrMap[i]=result.toDouble();
											}

											break;
										case 'N':    //If Minimum
											operand = (Expression) item.getParameters().getExpressions().get(0);
											result = e.eval(operand);
											if(result.toDouble() < aggrMap[i])
											{
												aggrMap[i] = result.toDouble();
											}

											break;
										case 'M':      //if Sum
											operand = (Expression) item.getParameters().getExpressions().get(0);
											result = e.eval(operand);
											if(result !=null)
											{
												aggrMap[i]+=result.toDouble();
											}
											break;																				
										case 'U':      //if Count
											if(!item.toString().toLowerCase().contains("count(*)"))
											{
												operand = (Expression) item.getParameters().getExpressions().get(0);
												result = e.eval(operand);
												if(result!=null)
												{
													if(e.eval(operand)!=null)
													{
														aggrMap[i]+=1;
													}
												}
											}
											break;
										case 'G':	   //If Average
											operand = (Expression) item.getParameters().getExpressions().get(0);
											result = e.eval(operand);
											if(result!=null)
											{
												aggrMap[i]+=result.toDouble();
												aggrDenomMap[i]+=1;
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
			}		
			
			//Update the Values to hasTable
			if(is_aggregate==true)
			{
				if(selectlist.size()!=0){
					aggregateHistory.get(tableName).get("COUNT_A").replace("COUNT",count_All);
				}
				item=null;
				for(int i =0; i<selectlist.size();i++)
				{
					item = (Function) selectlist.get(i);
					String columnName ="";
					if(item.toString().contains("(*)"))
					{
						columnName = "COUNT_A";
					}
					else
					{
						columnName= ((Function) selectlist.get(i)).getParameters().getExpressions().get(0).toString();
					}						
						

					if(selectlist.get(i) instanceof Function)
					{
						if ((item.getName().equals("SUM")
								||item.getName().equals("MIN")
								||item.getName().equals("COUNT")
								||item.getName().equals("MAX")) && !item.toString().toLowerCase().equals("count(*)"))
						{
							aggregateHistory.get(tableName).get(columnName).replace(item.getName(),aggrMap[i]);
							//System.out.println(aggregateHistory.get(tableName));
						}
						else if(item.getName().equals("AVG"))
						{
							aggregateHistory.get(tableName).get(columnName).replace(item.getName(),
									(aggrMap[i]/aggrDenomMap[i]));
							aggregateHistory.get(tableName).get(columnName).replace("SUM",aggrMap[i]);							
							aggregateHistory.get(tableName).get(columnName).replace("COUNT",(double) aggrDenomMap[i]);
						}						
					}
					else
					{// if its a Simple String Column or Simple Number Column
						Double elsecase = Double.parseDouble(aggrStrMap[i]);
						aggregateHistory.get(tableName).get(columnName).replace(item.getName(), elsecase);
					}
				}
				
				
				//Read the Data from hashMap and Print the output on Console
				for(int i =0; i<originalSelectList.size();i++)
				{
					String columnName ="";
					item = (Function) originalSelectList.get(i);
					if(item.toString().contains("(*)"))
					{
						columnName = "COUNT_A";
					}
					else
					{
						columnName = ((Function) originalSelectList.get(i)).getParameters().getExpressions().get(0).toString();
					}
					if(originalSelectList.get(i) instanceof Function){

						if(item.toString().toLowerCase().equals("count(*)"))
						{
							sb.append(aggregateHistory.get(tableName).get(columnName).get("COUNT").longValue()+"|");
						}
						else if(item.getName().equalsIgnoreCase("SUM")
								||item.getName().equalsIgnoreCase("MIN")
								||item.getName().equalsIgnoreCase("COUNT")
								||item.getName().equalsIgnoreCase("MAX"))
						{
							int index = 0 ;
							if(columnNameToIndexMapping.get(tableName).get(columnName)==null){
								index = -1;
								sb.append( aggregateHistory.get(tableName).get(columnName).get(item.getName()));
							}
							else {
								index = columnNameToIndexMapping.get(tableName).get(columnName);
								String type = columnDataTypes.get(tableName).get(index);
								Double temp= aggregateHistory.get(tableName).get(columnName).get(item.getName());
								sb.append((type.equals("long")||type.equals("int"))? temp.longValue()+"|":temp+"|");
							}
						}
						else if(item.getName().equalsIgnoreCase("AVG")){
							sb.append(aggregateHistory.get(tableName).get(columnName).get(item.getName())+"|");
						}
					}
					else{// if its a Simple String Column or Simple Number Column
						sb.append(aggregateHistory.get(tableName).get(columnName).get(item.getName()).toString()+"|");
					}
				}
			}
			if(sb.length() >=2)
			{

				sb.setLength(sb.length() - 1);
				sb.append("\n");
			}
			System.out.print(sb);
			Date d2 = new Date();
			long t2 = d2.getTime();
			System.out.println("time->"+(t2-t1));
		}
		catch(SQLException e){
			System.out.println(e.getMessage());

		}
	}
	static String[] colName = null;
	
	/*******************************************
	1> From Table Schema extract ColumnNames and ColumnDataTypes
	2> From File Corresponding to Table,read all the lines and Extract Common Statistics such as Min,Max and Average
	3> Save above extracted Values into corresponding HashMap
	*/

	public static void getColumnDataTypesAndMapColumnNameToIndex() throws SQLException, IOException
	{

		CreateTable create = (CreateTable)statement;
		String tableName = create.getTable().getName();
		//System.out.println(tableName);
		Map<String,Integer> columnNameToIndexMap = new HashMap<String,Integer>();
		new HashMap<String, Map<String,String>>();

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
		aggregateHistory.put(tableName, null);

		colName = new String[columnNameToIndexMapping.get(tableName).keySet().size()];
		columnNameToIndexMapping.get(tableName).keySet().toArray(colName);

		for(i =0; i<columnDataTypes.get(tableName).size();i++)
		{
			if(aggregateHistory.get(tableName)==null)
			{
				Map<String,Double> agg= new HashMap<String,Double>();
				agg.put("SUM",null);
				agg.put("MAX",null);
				agg.put("MIN",null);
				agg.put("AVG",null);
				agg.put("COUNT",null);
				Map<String,Map<String,Double>> col = new HashMap<String,Map<String,Double>>();
				col.put(colName[i], agg);
				aggregateHistory.put(tableName, col);
			}
			else if(aggregateHistory.get(tableName).get(colName[i])==null)
			{

				Map<String,Double> agg= new HashMap<String,Double>();
				agg.put("SUM",null);
				agg.put("MAX",null);
				agg.put("MIN",null);
				agg.put("AVG",null);
				agg.put("COUNT",null);
				aggregateHistory.get(tableName).put(colName[i], agg);
			}
		}


		BufferedReader brlocal = null;
		String csvFile_local_copy = csvFile+tableName+".csv";
		brlocal = new BufferedReader(new FileReader(String.format(csvFile_local_copy)));
		columnIndexArray = new String[columnNameToIndexMapping.get(tableName).keySet().size()];
		columnDataTypeArray = new String[columnDataTypes.get(tableName).size()];
		columnIndexList = Arrays.asList(columnIndexArray);
		Map<String, Integer> q = columnNameToIndexMapping.get(tableName);//.keySet()..toArray(columnIndexArray);
		columnDataTypes.get(tableName).toArray(columnDataTypeArray);
		for(String k: q.keySet()){
			columnIndexList.set(q.get(k),k);
		}
		int index = 0;
		double countAll = 0; 
		double [] minArr = new double[columnDataTypes.get(tableName).size()];
		double [] maxArr = new double[columnDataTypes.get(tableName).size()];
		double [] sumArr = new double[columnDataTypes.get(tableName).size()];
		double min=Integer.MAX_VALUE,max=Integer.MIN_VALUE;
		while((line=brlocal.readLine())!=null)
		{
			countAll++;
			
			rowData = line.split("\\|",-1);
			for(String colName : columnNameToIndexMapping.get(tableName).keySet())
			{
				columnNameToIndexMapping.get(tableName).keySet();
				index = columnIndexList.indexOf(colName);
				if(columnDataTypeArray[index].equals("int") 
						|| columnDataTypeArray[index].equals("long")
						|| columnDataTypeArray[index].equals("double"))
				{
					if(aggregateHistory.get(tableName).get(colName).get("MAX")==null){
						aggregateHistory.get(tableName).get(colName).put("MAX", Double.parseDouble(rowData[index]));
						maxArr[index] = Double.parseDouble(rowData[index]);
					}
					
					if(aggregateHistory.get(tableName).get(colName).get("MIN")==null){
						aggregateHistory.get(tableName).get(colName).put("MIN", Double.parseDouble(rowData[index]));
						minArr[index] = Double.parseDouble(rowData[index]);
					}
					if(aggregateHistory.get(tableName).get(colName).get("SUM")==null){
						aggregateHistory.get(tableName).get(colName).put("SUM", Double.parseDouble(rowData[index]));
						sumArr[index] = Double.parseDouble(rowData[index]);
					}
					
					if(minArr[index]>Double.parseDouble(rowData[index])){
						minArr[index] = Double.parseDouble(rowData[index]);
					}
					if(maxArr[index]<Double.parseDouble(rowData[index])){
						maxArr[index] = Double.parseDouble(rowData[index]);
					}
					sumArr[index]+=Double.parseDouble(rowData[index]);
					aggregateHistory.get(tableName).get(colName).put("MAX", maxArr[index]);
					aggregateHistory.get(tableName).get(colName).put("MIN", minArr[index]);
					aggregateHistory.get(tableName).get(colName).put("SUM", sumArr[index]);
				}
			
				
			}
		}
		
		
		if(aggregateHistory.get(tableName).get("COUNT_A")==null)
		{
			Map<String,Double> agg= new HashMap<String,Double>();
			agg.put("COUNT",countAll);					
			aggregateHistory.get(tableName).put("COUNT_A", agg);
		}


	}


	public static void readQueries(String temp) throws ParseException
	{

		StringReader input = new StringReader(temp);
		parser = new CCJSqlParser(input);
		statement = parser.Statement();  
	}

	static class EvalLib extends Eval{
		String tableName = "";
		int index = 0;

		public EvalLib(String tableName){
			this.tableName = tableName;
		}
		@Override
		public PrimitiveValue eval(Column col) throws SQLException {
			index = columnIndexList.indexOf(col.getColumnName());
			switch(columnDataTypeArray[index].toLowerCase())
			{
			case "int": 
				return new LongValue(rowData[index]);
				//return new LongValue(record.get(index));
			case "decimal":
				return new DoubleValue(rowData[index]);
				//return new DoubleValue(record.get(index));
			case "string":
			case "varchar":
			case "char":
				return new StringValue(rowData[index]);
				//return new StringValue(record.get(index));

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