/*
CREATE TABLE LINEITEM(ORDERKEY INT,PARTKEY INT,SUPPKEY INT,LINENUMBER INT,QUANTITY DECIMAL,EXTENDEDPRICE DECIMAL,DISCOUNT DECIMAL,TAX DECIMAL,RETURNFLAG CHAR(1),LINESTATUS CHAR(1),SHIPDATE DATE,COMMITDATE DATE,RECEIPTDATE DATE,SHIPINSTRUCT CHAR(25),SHIPMODE CHAR(10),PRIMARY KEY (ORDERKEY,LINENUMBER),INDEX LINEITEM_shipdate (shipdate)); SELECT LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS, SUM(LINEITEM.QUANTITY) AS SUM_QTY, SUM(LINEITEM.EXTENDEDPRICE) AS SUM_BASE_PRICE, SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)) AS SUM_DISC_PRICE, SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)*(1+LINEITEM.TAX)) AS SUM_CHARGE, AVG(LINEITEM.QUANTITY) AS AVG_QTY, AVG(LINEITEM.EXTENDEDPRICE) AS AVG_PRICE, AVG(LINEITEM.DISCOUNT) AS AVG_DISC, COUNT(*) AS COUNT_ORDER FROM LINEITEM WHERE LINEITEM.SHIPDATE <= DATE('1998-08-10') GROUP BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS ORDER BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS;
CREATE TABLE LINEITEM(ORDERKEY INT,PARTKEY INT,SUPPKEY INT,LINENUMBER INT,QUANTITY DECIMAL,EXTENDEDPRICE DECIMAL,DISCOUNT DECIMAL,TAX DECIMAL,RETURNFLAG CHAR(1),LINESTATUS CHAR(1),SHIPDATE DATE,COMMITDATE DATE,RECEIPTDATE DATE,SHIPINSTRUCT CHAR(25),SHIPMODE CHAR(10),PRIMARY KEY (ORDERKEY,LINENUMBER),INDEX LINEITEM_shipdate (shipdate));SELECT LINEITEM.EXTENDEDPRICE*1-LINEITEM.DISCOUNT from LINEITEM
SELECT IN_LINE.PARTKEY, IN_LINE.LINENUMBER FROM (SELECT (SUPPKEY+ORDERKEY) AS PARTKEY,(SUPPKEY*ORDERKEY) AS LINENUMBER FROM LINEITEM) IN_LINE
CREATE TABLE LINEITEM
(ORDERKEY INT,PARTKEY INT,SUPPKEY INT
,LINENUMBER INT,QUANTITY DECIMAL,EXTENDEDPRICE DECIMAL
,DISCOUNT DECIMAL,TAX DECIMAL,RETURNFLAG CHAR(1)
,LINESTATUS CHAR(1),SHIPDATE DATE,COMMITDATE DATE
,RECEIPTDATE DATE,SHIPINSTRUCT CHAR(25),SHIPMODE CHAR(10)
,PRIMARY KEY (ORDERKEY,LINENUMBER)
,INDEX LINEITEM_shipdate (shipdate));
CREATE TABLE LINEITEM2
(ORDERKEY INT,PARTKEY INT,SUPPKEY INT
,LINENUMBER INT,QUANTITY DECIMAL,EXTENDEDPRICE DECIMAL
,DISCOUNT DECIMAL,TAX DECIMAL,RETURNFLAG CHAR(1)
,LINESTATUS CHAR(1),SHIPDATE DATE,COMMITDATE DATE
,RECEIPTDATE DATE,SHIPINSTRUCT CHAR(25),SHIPMODE CHAR(10)
,PRIMARY KEY (ORDERKEY,LINENUMBER)
,INDEX LINEITEM_shipdate (shipdate));
SELECT
SUM(LINEITEM2.EXTENDEDPRICE*LINEITEM2.DISCOUNT) AS REVENUE
FROM
LINEITEM2
WHERE
LINEITEM2.SHIPDATE >= DATE('1995-01-01')
AND LINEITEM2.SHIPDATE < DATE ('1996-01-01')
AND LINEITEM2.DISCOUNT > 0.08 AND LINEITEM2.DISCOUNT < 0.1 
AND LINEITEM2.QUANTITY < 25;




SELECT
 *
FROM
LINEITEM
WHERE
LINEITEM.SHIPDATE >= DATE('1995-01-01')
AND LINEITEM.SHIPDATE < DATE ('1996-01-01')
AND LINEITEM.DISCOUNT > 0.08 AND LINEITEM.DISCOUNT < 0.1 
AND LINEITEM.QUANTITY < 25
LIMIT 10;

CREATE TABLE LINEITEM (ORDERKEY INT,PARTKEY INT,SUPPKEY INT ,LINENUMBER INT,QUANTITY DECIMAL,EXTENDEDPRICE DECIMAL ,DISCOUNT DECIMAL,TAX DECIMAL,RETURNFLAG CHAR(1) ,LINESTATUS CHAR(1),SHIPDATE DATE,COMMITDATE DATE ,RECEIPTDATE DATE,SHIPINSTRUCT CHAR(25),SHIPMODE CHAR(10) ,PRIMARY KEY (ORDERKEY,LINENUMBER) ,INDEX LINEITEM_shipdate (shipdate)); SELECT SUM(LINEITEM.EXTENDEDPRICE*LINEITEM.DISCOUNT) AS REVENUE FROM LINEITEM WHERE LINEITEM.SHIPDATE >= DATE('1994-01-01') AND LINEITEM.SHIPDATE < DATE ('1995-01-01') AND LINEITEM.DISCOUNT > 0.01 AND LINEITEM.DISCOUNT < 0.03 AND LINEITEM.QUANTITY < 25;


SELECT
SUM(LINEITEM.EXTENDEDPRICE*LINEITEM.DISCOUNT) AS REVENUE
FROM
LINEITEM
WHERE
LINEITEM.SHIPDATE >= DATE('1994-01-01')
AND LINEITEM.SHIPDATE < DATE ('1995-01-01')
AND LINEITEM.DISCOUNT > 0.01 AND LINEITEM.DISCOUNT < 0.03 
AND LINEITEM.QUANTITY < 25;

SELECT LINEITEM.EXTENDEDPRICE*1-LINEITEM.DISCOUNT 
from LINEITEM where LINEITEM.DISCOUNT>0.002;
SELECT
LINEITEM.RETURNFLAG,
LINEITEM.LINESTATUS,
SUM(LINEITEM.QUANTITY) AS SUM_QTY,
SUM(LINEITEM.EXTENDEDPRICE) AS SUM_BASE_PRICE, 
SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)) AS SUM_DISC_PRICE, 
SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)*(1+LINEITEM.TAX)) AS SUM_CHARGE, 
AVG(LINEITEM.QUANTITY) AS AVG_QTY,
AVG(LINEITEM.EXTENDEDPRICE) AS AVG_PRICE,
AVG(LINEITEM.DISCOUNT) AS AVG_DISC,
COUNT(*) AS COUNT_ORDER
FROM
LINEITEM
WHERE
LINEITEM.SHIPDATE <= DATE('1998-09-03')
GROUP BY 
LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS 
ORDER BY
LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS;

SELECT LINEITEM.RECEIPTDATE
, LINEITEM.PARTKEY
, LINEITEM.EXTENDEDPRICE FROM LINEITEM WHERE DATE('1997-01-01') < LINEITEM.SHIPDATE 
AND LINEITEM.SHIPDATE <= DATE('1998-01-01') 
LIMIT 10;

CREATE TABLE LINEITEM
(ORDERKEY INT,PARTKEY INT,SUPPKEY INT
,LINENUMBER INT,QUANTITY DECIMAL,EXTENDEDPRICE DECIMAL
,DISCOUNT DECIMAL,TAX DECIMAL,RETURNFLAG CHAR(1)
,LINESTATUS CHAR(1),SHIPDATE DATE,COMMITDATE DATE
,RECEIPTDATE DATE,SHIPINSTRUCT CHAR(25),SHIPMODE CHAR(10)
,PRIMARY KEY (ORDERKEY,LINENUMBER)
,INDEX LINEITEM_shipdate (shipdate));
SELECT LINEITEM.RECEIPTDATE
, LINEITEM.PARTKEY
, LINEITEM.EXTENDEDPRICE FROM LINEITEM WHERE DATE('1997-01-01') < LINEITEM.SHIPDATE 
AND LINEITEM.SHIPDATE <= DATE('1998-01-01') 
LIMIT 10;

 */
package dubstep;
import net.sf.jsqlparser.eval.Eval;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.Timer;
import java.util.TreeSet;

import javax.swing.plaf.synth.SynthSeparatorUI;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;

public class Main {

	private static Scanner scan;
	private String[] rowData = null; 
	private BufferedReader br = null;	
	private Reader in = null;
	private String csvFile = "src\\dubstep\\data\\";
	//public static String csvFile = "data/";
	private  String line = "";
	private  Statement statement;
	private  PlainSelect plain;
	private  Map<String,ArrayList <String>> columnDataTypes = new HashMap<String,ArrayList <String>>();
	private  Map<String,Map<String,Integer>> columnNameToIndexMapping = new HashMap<String,Map<String,Integer>>();
	private  Select select;
	private  SelectBody body;
	private  CCJSqlParser parser;

	public  void readQueries(String temp) throws ParseException
	{

		testRead();
		StringReader input = new StringReader(temp);
		parser = new CCJSqlParser(input);
		statement = parser.Statement();  
	}
	
	public  void testRead(){
		StringReader s1 = new StringReader("Select A from B where X = 5");
		StringReader s2 = new StringReader("Select A from B where X = 6");
		StringReader s3 = new StringReader("Select A from B where X = 7");
		StringReader s4 = new StringReader("Select A from B where X = 8");

	}
	public  void parseQueries() throws Exception
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
	public  void parseSelectStatement() throws Exception
	{

		select = (Select)statement;
		body = select.getSelectBody();


		if(body instanceof PlainSelect){

			plain = (PlainSelect)body;
			Table table = null;
			if(plain.getFromItem() instanceof SubSelect){
				setSubSelectCalls((SubSelect) plain.getFromItem());
			}
			else{// if(plain.getFromItem() instanceof Table){
				table = (Table) plain.getFromItem();
			}
			String tableName = table.getName();
			getSelectedColumns(tableName, plain.getWhere());

		}

		else {
			throw new Exception("I can't understand body instanceof PlainSelect "+statement);
		}
		/** Do something with the select operator **/

	}

	public  void setSubSelectCalls(SubSelect subQuery) throws Exception{

		SelectBody selBody = subQuery.getSelectBody();
		PlainSelect subqPlain = null;
		Table tb = new Table();

		if(selBody instanceof PlainSelect){
			try {
				subqPlain = (PlainSelect)selBody;
				Table table = null;
				if(subqPlain.getFromItem() instanceof SubSelect){
					setSubSelectCalls((SubSelect) subqPlain.getFromItem());
				}
				else{// if(plain.getFromItem() instanceof Table){
					table = (Table) subqPlain.getFromItem();
					String tableName = table.getName();
					getSelectedColumns(tableName, subqPlain.getWhere());
				}
			} catch (InvalidPrimitive e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else {
			throw new Exception("I can't understand body instanceof PlainSelect "+statement);
		}

		/** Do something with the select operator **/


	}
	public  void getColumnDataTypesAndMapColumnNameToIndex() throws SQLException
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






	public  void getSelectedColumns(String tableName, Expression whereExpression) throws IOException, InvalidPrimitive, SQLException
	{

		Map<String,ArrayList<String>> groupByStringMap = new HashMap<String,ArrayList<String>>();
		Map<String,ArrayList<Double>> groupByMap = new HashMap<String,ArrayList<Double>>();
		Map<String,ArrayList<Integer>> groupByMapDenominators = new HashMap<String,ArrayList<Integer>>();
		EvalLib e = new EvalLib(tableName);
		List<Column> groupByColumns = plain.getGroupByColumnReferences();
		ArrayList <Expression> selectlist = new ArrayList<Expression>();
		List<OrderByElement> orderByElements = plain.getOrderByElements();

		List<SelectItem> selectItems = plain.getSelectItems();
		String csvFile_local_copy = csvFile+tableName+".csv";
		//PlainSelect ps = new CCJSqlParser(new StringReader("Select")).PlainSelect();
		//"SELECT "+  +" FROM "+tableName
		br = new BufferedReader(new FileReader(String.format(csvFile_local_copy)));
		StringBuilder sb = new StringBuilder();	

		//		sb.append(String
		//				.join(
		//						System.getProperty("line.separator")
		//						,Files.readAllLines(Paths.get(csvFile_local_copy))
		//						)
		//				);
		boolean isStarPresent = false;
		boolean isAggregate=false;
		for(SelectItem select: selectItems)
		{
			//System.out.println(select);

			if(select.toString().equals("*")){ 

				isStarPresent = true; break;

			}

			Expression expression = ((SelectExpressionItem)select).getExpression();
			if(expression instanceof Function)
			{
				isAggregate = true;
				selectlist.add(((Function) expression));
			}
			else{
				selectlist.add(expression);
			}
		}
		PrimitiveValue result=null;
		boolean whereclauseabsent = (plain.getWhere()==null)?true:false;


		ArrayList<String> avoidDuplicates = new ArrayList<String>();
		ArrayList<Expression> l = new ArrayList<Expression>();
		//Remove Duplicates

		Map<String,Boolean> duplicateMap = new HashMap<String,Boolean>();  
		for(int idx = 0 ; idx<selectlist.size();idx++)
		{
			//duplicateMap.put(selectlist.get(2).toString(),false);
			if(duplicateMap.get(selectlist.get(idx).toString().replace("SUM(", "AVG("))!=null)
			{				
				duplicateMap.put(selectlist.get(idx).toString(),true);
			}
			else if(duplicateMap.get(selectlist.get(idx).toString().replace("AVG(", "SUM("))!=null)
			{
				duplicateMap.put(selectlist.get(idx).toString().replace("AVG(", "SUM("),true);
			}
			else {

			}
		}
		Map<String,Double> aggrMap = new HashMap<String,Double>(); 
		aggrMap.put("SUM",0.0);
		aggrMap.put("COUNT", 0.0);
		aggrMap.put("MIN", 0.0);
		aggrMap.put("MAX", 0.0);
		boolean simplePrint = false;
		int index=0;
		int groupRowCount=0;
		String temp = "";
		long lineCounter=0;
		String groupKey = "";
		if(isStarPresent && whereclauseabsent){
			sb.append(String
					.join(
							System.getProperty("line.separator")
							,Files.readAllLines(Paths.get(csvFile_local_copy))
							)
					);
		}
		else if(whereclauseabsent){
			lineCounter =0;
			while((line=br.readLine())!=null)
			{
				lineCounter++;
				rowData = line.split("\\|",-1);
				for(int i=0;i<selectItems.size();i++)
				{
					result = e.eval(((SelectExpressionItem)selectItems.get(i)).getExpression());
					sb.append(result.toRawString().concat("|"));
				}
				sb.setLength(sb.length() - 1); 
				sb.append("\n");
				if(lineCounter==plain.getLimit().getRowCount())
					break;
			}
		}
		else if(isStarPresent && !whereclauseabsent){
			lineCounter=0;
			while((line=br.readLine())!=null)
			{

				rowData = line.split("\\|",-1);
				if(e.eval(whereExpression).toBool()){
					lineCounter++;
					sb.append(line); 
					sb.append("\n");
					if(plain.getLimit()!=null && lineCounter==plain.getLimit().getRowCount())
						break;
				}				

			}
		}
		else if(isAggregate && (groupByColumns==null)){// if no group by but aggregate is present // old code			

			
			Thread m = new MyThread2(null,tableName,whereExpression,columnDataTypes,columnNameToIndexMapping, plain);
			m.start();
			
			Thread m2 = new MyThread2(tableName,tableName+"2",whereExpression,columnDataTypes,columnNameToIndexMapping, plain);
			m2.start();
			
//						Aggregate a = new Aggregate();
//						a.setColumnDataTypes(columnDataTypes);
//						a.setColumnNameToIndexMapping(columnNameToIndexMapping);
//						a.setPlain(plain);
//						a.getSelectedColumns(tableName, whereExpression);
//					//	Aggregate.getSelectedColumns(tableName+"2", whereExpression);
						simplePrint = true;
		}
		else {//if group by is present and where is present
			lineCounter = 0;
			//Timer t = new Timer();
			Date d = new Date();

			int hrs = d.getHours();
			int min = d.getMinutes();
			int sec = d.getSeconds();
			while((line=br.readLine())!=null)
			{
				groupKey="";
				rowData = line.split("\\|",-1);
				if(e.eval(whereExpression).toBool())
				{
					lineCounter++;
					//System.out.println(lineCounter);
					if(groupByColumns!=null){
						for(int i=0;i<groupByColumns.size()-1;i++){
							groupKey+=e.eval(groupByColumns.get(i)).toRawString()+"|";
						}
						groupKey+=e.eval(groupByColumns.get(groupByColumns.size()-1)).toRawString();
						if(groupByMap.get(groupKey)==null){
							groupByMapDenominators.put(groupKey,new ArrayList<Integer>());
							groupByMap.put(groupKey,new ArrayList<Double>()); 
							groupByStringMap.put(groupKey,new ArrayList<String>());
							for(int i =0; i<selectlist.size();i++)
							{
								groupByMap.get(groupKey).add(0.0);
								groupByMapDenominators.get(groupKey).add(0);
								groupByStringMap.get(groupKey).add("");
							}
						}
					}
					//Key Should be generated according to group order by

					//if(e.eval(((SelectExpressionItem)selectItems.get(i)).getExpression()))
					if(!isAggregate && (groupByColumns==null)){
						for(int i=0;i<selectItems.size();i++)
						{

							result = e.eval(((SelectExpressionItem)selectItems.get(i)).getExpression());
							sb.append(result.toRawString().concat("|"));
						}
						sb.setLength(sb.length() - 1);
						sb.append("\n");
						if(plain.getLimit()!=null && lineCounter==plain.getLimit().getRowCount())
							break;
					}
					else{

						for(int i =0; i<selectlist.size();i++)
						{
							Function func = null;
							Expression operand = null;
							if(selectlist.get(i) instanceof Function){
								func = (Function) selectlist.get(i);
								switch (func.getName().charAt(2)){
								case 'G'://AVG										
									result = e.eval(func.getParameters().getExpressions().get(0));
									if(result!=null)
									{
										groupByMap.get(groupKey).set(i, groupByMap.get(groupKey).get(i)+result.toDouble());
										groupByMapDenominators.get(groupKey).set(i, groupByMapDenominators.get(groupKey).get(i)+1);
									}
									break;
								case 'M'://SUM
									result = e.eval(func.getParameters().getExpressions().get(0));
									if(result!=null)
									{
										groupByMap.get(groupKey).set(i, groupByMap.get(groupKey).get(i)+result.toDouble());
									}
									break;
								case 'U'://COUNT
									if(func.toString().contains("(*)"))
									{
										groupByMap.get(groupKey).set(i, groupByMap.get(groupKey).get(i)+1);
									}
									else{
										operand = (Expression) func.getParameters().getExpressions().get(0);
										result = e.eval(operand);
										//index =columnNameToIndexMapping.get(tableName).get(operand.toString());
										temp =result.toRawString();//record.get(index);//(rowData[index]);
										if(temp.trim().length() != 0)
										{
											//result = e.eval(operand);
											if(e.eval(operand)!=null)
											{
												groupByMap.get(groupKey).set(i, groupByMap.get(groupKey).get(i)+1);
											}
										}
									}
									break;
								case 'N'://MIN
									operand = (Expression) func.getParameters().getExpressions().get(0);
									result = e.eval(operand);
									//index =columnNameToIndexMapping.get(tableName).get(operand.toString());
									temp =result.toRawString();//record.get(index);//(rowData[index]);
									if(temp.trim().length() != 0)
									{
										//result = e.eval(operand);
										if(result.toDouble() < groupByMap.get(groupKey).get(i))
										{
											groupByMap.get(groupKey).set(i,result.toDouble());
										}
									}
									break;
								case 'X'://MAX
									operand = (Expression) func.getParameters().getExpressions().get(0);
									result = e.eval(operand);
									//index =columnNameToIndexMapping.get(tableName).get(operand.toString());
									temp =result.toRawString();//record.get(index);//(rowData[index]);
									if(temp.trim().length() != 0)
									{
										//result = e.eval(operand);
										if(result.toDouble() > groupByMap.get(groupKey).get(i))
										{
											groupByMap.get(groupKey).set(i,result.toDouble());
										}
									}
									break;
								default:
									break;
								}

							}
							else{// If the Column is not an aggregate column then simply get the value
								operand = selectlist.get(i);
								if(groupByStringMap.get(groupKey).get(i).trim().length()==0){
									groupByStringMap.get(groupKey).set(i,e.eval(operand).toRawString());
								}
							}
						}
					}

					//result = e.eval(((SelectExpressionItem)selectItems.get(selectItems.size()-1)).getExpression());
					//sb.append(result.toRawString()+"\n");

				}
			}
			d = new Date();
			int hrs1 = d.getHours();
			int min1 = d.getMinutes();
			int sec1 = d.getSeconds();
			//System.out.println("time to execute gropu by = "+ (hrs1-hrs)+":"+(min1-min)+":"+(sec1-sec));
		}
		if(isAggregate && !simplePrint)
		{
			//for orderby implement logic // later
			List<String> keyList = new ArrayList<String>(groupByMap.keySet());
			Collections.sort(keyList);
			//
			for(String outputGroupKey: keyList){

				for(int i =0; i<selectlist.size();i++)
				{
					Function item = null;
					if(selectlist.get(i) instanceof Function){
						item = (Function) selectlist.get(i);
						switch(item.getName()){
						case "SUM":
						case "MIN":
						case "MAX":							
							sb.append((result instanceof LongValue)?((groupByMap.get(outputGroupKey).get(i).longValue())+"|"):groupByMap.get(outputGroupKey).get(i)+"|");
							break;
						case "count":
						case "COUNT":
							sb.append(groupByMap.get(outputGroupKey).get(i).longValue()+"|");
							break;
						case "AVG":
							sb.append((groupByMap.get(outputGroupKey).get(i)/groupByMapDenominators.get(outputGroupKey).get(i))+"|");
							break;
						default: break;
						}
					}
					else{// if its a Simple String Column or Simple Number Column
						sb.append(groupByStringMap.get(outputGroupKey).get(i)+"|");
					}
				}
				sb.setLength(sb.length() - 1);
				sb.append("\n");
			}
			if(sb.length() >=2)
			{

				sb.setLength(sb.length() - 1);
				sb.append("\n");
			}
		}
		else{
			if(orderByElements!=null && orderByElements.size()>0){
				orderElements();
			}
		}
		//System.out.println(groupByMap);
		//sb.setLength(sb.length() - 1);
		System.out.println(sb.toString());//to print normal queries
	}
	public  void orderElements(){

	}
	

	public static void main(String[] args) throws Exception {
		Main main = new Main();
		System.out.print("$>");
		scan = new Scanner(System.in);
		String temp;
		String query = "";
		while((temp = scan.nextLine()) != null)
		{
			query+=temp+" ";
			if(temp.indexOf(';')>=0){
				main.readQueries(query);
				main.parseQueries();
				System.out.print("$>");
				query="";
			}

		}
		scan.close();

	}



	 class EvalLib extends Eval{
		String tableName = "";
		public EvalLib(String tableName){
			this.tableName = tableName;
		}
		@Override
		public PrimitiveValue eval(Column col) throws SQLException {
			int index =columnNameToIndexMapping.get(tableName).get(col.getColumnName());
			switch(columnDataTypes.get(tableName).get(index).toLowerCase())
			{
			case "string":
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

	public class MyRunnable implements Runnable {
		public void run() {
			System.out.println(" Create Thread " + Thread.currentThread().getName());
		}
	}


}

class MyThread2 extends Thread {
	private Thread t=null ;
	
	public String tableName = "";
	public Expression whereExpression = null;
	public String originalTable = "";
	public PlainSelect plain= null;	
	public Map<String,Map<String,Integer>> colNameToIndexMapping = null;
	public Map<String, ArrayList<String>> colDataTypes = null;
	
	public void start(){
		if (t == null) {
	         t = new Thread (this, tableName);
	         t.start ();
	      }

	}
	
	public MyThread2(String originalTable, String tableName,Expression whereExpression
			, Map<String, ArrayList<String>> colDataTypes
			,Map<String,Map<String,Integer>> colNameToIndexMapping
			,PlainSelect plain) {
		super("MyThread2");
		this.tableName = tableName;
		this.whereExpression = whereExpression;
		this.originalTable = originalTable;
		this.colDataTypes = colDataTypes;
		this.colNameToIndexMapping =colNameToIndexMapping;
		this.plain = plain;
	}
	public void run() {
		try{
			
				
			if(originalTable!=null){
				Date d = new Date();
				long t1 = d.getTime();
				
				System.out.println("-------T1-----");
//				for(int i=0;i<100;i++){
//					System.out.println("T1->"+i);
//					Thread.sleep(50);
//				}
				final ThreadLocal<Aggregate> threadId =
						new ThreadLocal<Aggregate>() {
					@Override protected Aggregate initialValue() {
						return new Aggregate();
					}
				};
				try{
					Map<String, ArrayList<String>> a2MapDT= new HashMap<String, ArrayList<String>>();
					a2MapDT.put(tableName, colDataTypes.get(originalTable));
					Map<String, Map<String,Integer>> a2MapIndex= new HashMap<String, Map<String,Integer>>();
					a2MapIndex.put(tableName, colNameToIndexMapping.get(originalTable));				
					
					threadId.get().setColumnNameToIndexMapping(a2MapIndex);
					threadId.get().setColumnDataTypes(a2MapDT);
					threadId.get().setPlain(plain);
					threadId.get().getSelectedColumns(tableName, whereExpression);
					

				}
				catch(Exception ex){
					ex.printStackTrace();
				}
				d = new Date();
				long t2 = d.getTime();
				System.out.println(tableName+"->Time taken->"+(t2-t1));
			}
			//http://www.tutorialspoint.com/java/java_multithreading.htm
			else{
				Date d = new Date();
				long t1 = d.getTime();
				try{
					System.out.println("-------T2-----");
//								for(int i=0;i<100;i++){
//									System.out.println("T2->"+i);
//									Thread.sleep(50);
//								}

					final ThreadLocal<Aggregate> threadId =
							new ThreadLocal<Aggregate>() {
						@Override protected Aggregate initialValue() {
							return new Aggregate();
						}
					};

					threadId.get().setColumnDataTypes(colDataTypes);
					threadId.get().setColumnNameToIndexMapping(colNameToIndexMapping);
					threadId.get().setPlain(plain);
					threadId.get().getSelectedColumns(tableName, whereExpression);
					//threadId.get().func2(originalTable,tableName, whereExpression);
				}
				catch(Exception ex){
					ex.printStackTrace();
				}
				d = new Date();
				long t2 = d.getTime();
				System.out.println(tableName+"->Time taken->"+(t2-t1));
			}
			
			
			
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
	}
	//	public static void setTableName(String tableName) {
	//		MyThread1.tableName = tableName;
	//	}
	//	public static void setWhereExpression(Expression whereExpression) {
	//		MyThread1.whereExpression = whereExpression;
	//	}

} 

//class MyThread1 extends Thread {
//	public static String tableName = "";
//	public static String originalTable = "";
//	public static Expression whereExpression = null;
//	public static PlainSelect plain= null;	
//	public static Map<String,Map<String,Integer>> colNameToIndexMapping = null;
//	public static  Map<String, ArrayList<String>> colDataTypes = null;
//	public MyThread1(String originalTable, String tableName,Expression whereExpression
//			, Map<String, ArrayList<String>> colDataTypes
//			,Map<String,Map<String,Integer>> colNameToIndexMapping
//			,PlainSelect plain) {
//		super("MyThread1");
//		MyThread1.tableName = tableName;
//		MyThread1.whereExpression = whereExpression;
//		MyThread1.originalTable = originalTable;
//		MyThread1.colDataTypes = colDataTypes;
//		MyThread1.colNameToIndexMapping =colNameToIndexMapping;
//		MyThread1.plain = plain;
//	}
//	public void run() {
//		
//	}
//	//	public static void setTableName(String tableName) {
//	//		MyThread1.tableName = tableName;
//	//	}
//	//	public static void setWhereExpression(Expression whereExpression) {
//	//		MyThread1.whereExpression = whereExpression;
//	//	}
//
//} 


