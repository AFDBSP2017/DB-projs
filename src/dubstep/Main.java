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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;

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

	public static Scanner scan;
	static String[] rowData = null; 
	public static BufferedReader br = null;	
	static Reader in = null;
	public static String csvFile = "src\\dubstep\\data\\";
	//public static String csvFile = "data/";
	public static String line = "";
	public static Statement statement;
	public static PlainSelect plain;
	public static Map<String,ArrayList <String>> columnDataTypes = new HashMap<String,ArrayList <String>>();
	public static Map<String,Map<String,Integer>> columnNameToIndexMapping = new HashMap<String,Map<String,Integer>>();
	public static Select select;
	public static SelectBody body;
	public static CCJSqlParser parser;

	public static void readQueries(String temp) throws ParseException
	{

		StringReader input = new StringReader(temp);
		parser = new CCJSqlParser(input);
		statement = parser.Statement();  
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

	public static void setSubSelectCalls(SubSelect subQuery) throws Exception{

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

	public static void getSelectedColumns(String tableName, Expression whereExpression) throws IOException, InvalidPrimitive, SQLException
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


		//
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
		else {
			lineCounter = 0;
			while((line=br.readLine())!=null)
			{
				groupKey="";
				rowData = line.split("\\|",-1);
				if(e.eval(whereExpression).toBool())
				{
					lineCounter++;
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
					if(!isAggregate){
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
					else{

						for(int i =0; i<selectlist.size();i++)
						{
							Function func = null;
							Expression operand = null;
							if(selectlist.get(i) instanceof Function){
								func = (Function) selectlist.get(i);
								switch (func.getName().toLowerCase()){
								case "avg":										
									result = e.eval(func.getParameters().getExpressions().get(0));
									//index =columnNameToIndexMapping.get(tableName).get(operand.toString().split(".")[1]);
									//temp =record.get(index);
									if(result!=null)
									{
										groupByMap.get(groupKey).set(i, groupByMap.get(groupKey).get(i)+result.toDouble());
										groupByMapDenominators.get(groupKey).set(i, groupByMapDenominators.get(groupKey).get(i)+1);

									}
									break;
								case "sum":
									result = e.eval(func.getParameters().getExpressions().get(0));
									if(result!=null)
									{
										groupByMap.get(groupKey).set(i, groupByMap.get(groupKey).get(i)+result.toDouble());
									}
									break;
								case "count":
									if(func.toString().toLowerCase().contains("count(*)"))
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
								case "min":
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
								case "max":
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

		}
		if(isAggregate)
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

		}
		if(sb.length() >=2)
		{

			sb.setLength(sb.length() - 1);
			sb.append("\n");
		}
		//System.out.println(groupByMap);
		//sb.setLength(sb.length() - 1);
		System.out.println(sb.toString());//to print normal queries
	}

	public static Expression inExpression(InExpression exp){


		return null;

	}

	public static void main(String[] args) throws Exception {
		System.out.print("$>");
		scan = new Scanner(System.in);
		String temp;
		String query = "";
		while((temp = scan.nextLine()) != null)
		{
			query+=temp+" ";
			if(temp.indexOf(';')>=0){
				readQueries(query);
				parseQueries();
				System.out.print("$>");
				query="";
			}
			
		}
		scan.close();

	}



	static class EvalLib extends Eval{
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

				Date date=null;
				try {
					date = new SimpleDateFormat("yyyy-MM-ddd").parse(rowData[index]);
				} catch (java.text.ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return new DateValue(new SimpleDateFormat("yyyy-MM-dd").format(date));
				//return new DateValue(record.get(index));
			default:
				return new StringValue(rowData[index]);
				//return new StringValue(record.get(index));
			}
		}
	}
}