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

SELECT COUNT(*) AS OUTPUT_1
, MAX(LINEITEM.EXTENDEDPRICE + 3.982699713217688 + 0.08729368319077113 + LINEITEM.EXTENDEDPRICE + LINEITEM.ORDERKEY) AS OUTPUT_2 
FROM LINEITEM;

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

<shipdate,<orderkey+linenumber,rest of the cols>>
12345,1,2,jyoti1
12345,1,3,jyoti2
12345,2,4,jyoti3
12346,3,4

<12345,<1+2><1+3><2+4>>



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

import java.awt.Point;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.instrument.Instrumentation;
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
import java.util.SortedMap;
import java.util.Timer;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

import javax.swing.plaf.synth.SynthSeparatorUI;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import java.lang.instrument.Instrumentation;


public class Main {

	private  Map<String,Map<String,Map<String,Double>>> aggregateHistory=new HashMap<String,Map<String,Map<String,Double>>> ();
	private static Scanner scan;
	private String[] rowData = null;
	private BufferedReader br = null;
	private Reader in = null;
	private String csvFile = "src\\dubstep\\data\\";
	// public static String csvFile = "data/";
	private String line = "";
	private Map<String, List<String>> indexNameToColumnsMap = new HashMap<String, List<String>>();
	private Statement statement;
	private PlainSelect plain;
	private Map<String, ArrayList<String>> columnDataTypes = new HashMap<String, ArrayList<String>>();
	private Map<String, Map<String, Integer>> columnNameToIndexMapping = new HashMap<String, Map<String, Integer>>();
	private Select select;
	private SelectBody body;
	private CCJSqlParser parser;
	//private Map<Integer, HashMap<Object, List<Object>>> indexToPrmryKeyMap = new HashMap<Integer,HashMap<Object,List<Object>>>();
	private Map<Object, List<Object>> indexToPrmryKeyMap = new HashMap<Object,List<Object>>();


	private Map<Object,Object> primaryKeyToDataMap = new TreeMap<Object,Object>();

	public void readQueries(String temp) throws ParseException {
		StringReader input = new StringReader(temp);
		parser = new CCJSqlParser(input);
		statement = parser.Statement();
	}

	public void parseQueries() throws Exception {

		while (statement != null) {
			if (statement instanceof CreateTable) {				
				getColumnDataTypesAndMapColumnNameToIndex();
			} else if (statement instanceof Select) {

				parseSelectStatement();
			} else {
				throw new Exception("I can't understand statement instanceof Select" + statement);
			}
			statement = parser.Statement();

		}
	}

	public void parseSelectStatement() throws Exception {

		select = (Select) statement;
		body = select.getSelectBody();

		if (body instanceof PlainSelect) {

			plain = (PlainSelect) body;
			Table table = null;
			if (plain.getFromItem() instanceof SubSelect) {
				setSubSelectCalls((SubSelect) plain.getFromItem());
			} else {// if(plain.getFromItem() instanceof Table){
				table = (Table) plain.getFromItem();
			}
			String tableName = table.getName();
			getSelectedColumns(tableName, plain.getWhere());

		}

		else {
			throw new Exception("I can't understand body instanceof PlainSelect " + statement);
		}
		/** Do something with the select operator **/

	}

	public void createColumnIndexes(String tableName) {
		String csvFile_local_copy = csvFile + tableName + ".csv";
		String dataType = "";		
		
		try {
			br = new BufferedReader(new FileReader(String.format(csvFile_local_copy)));
			CreateTable create = (CreateTable) statement;
			//int[] indexColNum = new int[create.getColumnDefinitions().size()];
			for (Index i : create.getIndexes()) 
			{
				if(i.getName()==null && i.getColumnsNames().size()!=0)
				{
					indexNameToColumnsMap.put(i.getType(),i.getColumnsNames());
				}
				else
				{
					indexNameToColumnsMap.put(i.getType()+"_"+ String.join("_", i.getColumnsNames()),i.getColumnsNames());
					//indexNameToColumnsMap.put(i.getColumnsNames().get(0),i.getColumnsNames());
				}
			}
			String line32=""; 
			
			if(indexNameToColumnsMap.keySet().size()>0){
				for(String s: indexNameToColumnsMap.keySet())
				{

					
					if(indexNameToColumnsMap.keySet().contains("PRIMARY KEY") && s.contains("INDEX")){
						System.out.println("Processing Indexes..");
						
						int index = columnNameToIndexMapping.get(tableName).get(indexNameToColumnsMap.get(s).get(0).toUpperCase());
						int pmKeyIndexCol1 = columnNameToIndexMapping.get(tableName).get(indexNameToColumnsMap.get("PRIMARY KEY").get(0).toUpperCase());
						int pmKeyIndexCol2 = columnNameToIndexMapping.get(tableName).get(indexNameToColumnsMap.get("PRIMARY KEY").get(1).toUpperCase());
						int lineNum=0;
						Runtime.getRuntime().freeMemory();
						Runtime.getRuntime().gc();
						while ((line = br.readLine()) != null) {
							lineNum++;
							rowData = line.split("\\|",-1);
							if(rowData[index].length()>0){
								if(indexToPrmryKeyMap.get(rowData[index])==null){
									indexToPrmryKeyMap.put(rowData[index], new ArrayList<Object>());
									indexToPrmryKeyMap.get(rowData[index]).add(rowData[pmKeyIndexCol1]+","+rowData[pmKeyIndexCol2]);
								}
								else{
									indexToPrmryKeyMap.get(rowData[index]).add(rowData[pmKeyIndexCol1]+","+rowData[pmKeyIndexCol2]);
								}							
							}
							primaryKeyToDataMap.put(rowData[pmKeyIndexCol1]+","+rowData[pmKeyIndexCol2],rowData);

						}
						System.out.println("Indexes applied Successfully");
						//System.out.println("Writing Indexes to file...");
						double heapSize = (double)Runtime.getRuntime().totalMemory()/(double)(1024 * 1024); 
						// Get maximum size of heap in bytes. The heap cannot grow beyond this size.// Any attempt will result in an OutOfMemoryException.
						//d heapMaxSize = Runtime.getRuntime().maxMemory();
						// Get amount of free memory within the heap in bytes. This size will increase // after garbage collection and decrease as new objects are created.
						double heapFreeSize = (double)Runtime.getRuntime().freeMemory()/(double)(1024*1024); 
						System.out.println(heapFreeSize);
						System.out.println(heapFreeSize);
						//					FileOutputStream fout = new FileOutputStream("src\\dubstep\\data\\"+tableName+"_primaryKey");
						//					ObjectOutputStream oos = new ObjectOutputStream(fout);
						//					oos.writeObject(primaryKeyToDataMap);
						//					oos.flush();
						//					oos.close();
						//					System.out.println("Indexes successfully written");
						//					System.out.println("Reading Index back to oject");

						//					FileInputStream fin = new FileInputStream("src\\dubstep\\data\\"+tableName+"_primaryKey");
						//					ObjectInputStream ois = new ObjectInputStream(fin);
						//					Map<Object,Object> primaryKeyToDataMap_read = (TreeMap<Object,Object>)ois.readObject();
						//					System.out.println("Successfully read Index back to oject");
					}
				}
				//			try (Stream<String> lines = Files.lines(Paths.get(csvFile_local_copy))) {
				//			    line32 = lines.skip(2500).findFirst().get();
				//			    System.out.println(line32);
				//			}
			}
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// System.out.println(ObjectSizeFetcher.getObjectSize(list));
	}

	public void setSubSelectCalls(SubSelect subQuery) throws Exception {

		SelectBody selBody = subQuery.getSelectBody();
		PlainSelect subqPlain = null;
		Table tb = new Table();

		if (selBody instanceof PlainSelect) {
			try {
				subqPlain = (PlainSelect) selBody;
				Table table = null;
				if (subqPlain.getFromItem() instanceof SubSelect) {
					setSubSelectCalls((SubSelect) subqPlain.getFromItem());
				} else {// if(plain.getFromItem() instanceof Table){
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
		} else {
			throw new Exception("I can't understand body instanceof PlainSelect " + statement);
		}

		/** Do something with the select operator **/

	}

	public void getColumnDataTypesAndMapColumnNameToIndex() throws SQLException {

		Runtime.getRuntime().freeMemory();
		Runtime.getRuntime().gc();
		double heapSize = (double)Runtime.getRuntime().totalMemory()/(double)(1024 * 1024); 
		// Get maximum size of heap in bytes. The heap cannot grow beyond this size.// Any attempt will result in an OutOfMemoryException.
		//d heapMaxSize = Runtime.getRuntime().maxMemory();
		// Get amount of free memory within the heap in bytes. This size will increase // after garbage collection and decrease as new objects are created.
		double heapFreeSize = (double)Runtime.getRuntime().freeMemory()/(double)(1024*1024);
		System.out.println(heapFreeSize);
		System.out.println(heapFreeSize);
		CreateTable create = (CreateTable) statement;
		String tableName = create.getTable().getName();
		// System.out.println(tableName);
		Map<String, Integer> columnNameToIndexMap = new HashMap<String, Integer>();
		List<ColumnDefinition> si = create.getColumnDefinitions();
		ListIterator<ColumnDefinition> it = si.listIterator();
		ArrayList<String> dataTypes = new ArrayList<String>();

		int i = 0;
		while (it.hasNext()) {
			ColumnDefinition cd = it.next();
			dataTypes.add(cd.getColDataType().toString());
			// System.out.println("type = "+ dataTypes.get(i));
			columnNameToIndexMap.put(cd.getColumnName(), i++);
		}
		columnDataTypes.put(tableName, dataTypes);
		columnNameToIndexMapping.put(tableName, columnNameToIndexMap);
		List<Index> indexList = ((CreateTable) statement).getIndexes();		
		if (indexList!=null) {// can have duplicates
			createColumnIndexes(tableName);
		}
	}

	public void getSelectedColumns(String tableName, Expression whereExpression)
			throws IOException, InvalidPrimitive, SQLException {

		Map<String, ArrayList<String>> groupByStringMap = new HashMap<String, ArrayList<String>>();
		Map<String, ArrayList<Double>> groupByMap = new HashMap<String, ArrayList<Double>>();
		Map<String, ArrayList<Integer>> groupByMapDenominators = new HashMap<String, ArrayList<Integer>>();
		EvalLib e = new EvalLib(tableName);
		List<Column> groupByColumns = plain.getGroupByColumnReferences();
		ArrayList<Expression> selectlist = new ArrayList<Expression>();
		List<OrderByElement> orderByElements = plain.getOrderByElements();
		double[] aggrMap = new double[10];
		String[] aggrStrMap = new String[10];
		int[] aggrDenomMap = new int[10];
		List<SelectItem> selectItems = plain.getSelectItems();
		String csvFile_local_copy = csvFile + tableName + ".csv";
		// PlainSelect ps = new CCJSqlParser(new
		// StringReader("Select")).PlainSelect();
		// "SELECT "+ +" FROM "+tableName
		br = new BufferedReader(new FileReader(String.format(csvFile_local_copy)));
		StringBuilder sb = new StringBuilder();
		double count_All=0;
		int innerCount = 0;
		Function  item= null;
		Expression operand = null;
		// sb.append(String
		// .join(
		// System.getProperty("line.separator")
		// ,Files.readAllLines(Paths.get(csvFile_local_copy))
		// )
		// );
		boolean isStarPresent = false;
		boolean isAggregate = false;

		ArrayList<Expression> originalSelectList = new ArrayList<Expression>();
		for (SelectItem select : selectItems) {
			// System.out.println(select);

			if (select.toString().equals("(*)")) {

				isStarPresent = true;
				break;

			}

			Expression expression = ((SelectExpressionItem) select).getExpression();

			if (expression instanceof Function) {
				isAggregate = true;
				selectlist.add(((Function) expression));
			} else {
				selectlist.add(expression);
			}
			originalSelectList.add(((SelectExpressionItem) select).getExpression());
			// avoidDuplicates.add(((SelectExpressionItem)select).getExpression().toString())
		}
		PrimitiveValue result = null;
		boolean whereclauseabsent = (plain.getWhere() == null) ? true : false;

		ArrayList<String> avoidDuplicates = new ArrayList<String>();
		ArrayList<Expression> l = new ArrayList<Expression>();
		// Remove Duplicates

		ArrayList<Integer> indicesToRemove = new ArrayList<Integer>();
		//		if (groupByColumns != null) {
		//			for (int idx = 0; idx < originalSelectList.size(); idx++) {
		//				// duplicateMap.put(selectlist.get(2).toString(),false);
		//				String t = selectlist.get(idx).toString();
		//				if (selectlist.get(idx) instanceof Function) {
		//					if (originalSelectList.get(idx).toString().contains("AVG(") && originalSelectList.toString()
		//							.contains(originalSelectList.get(idx).toString().replace("AVG(", "SUM("))) {
		//						indicesToRemove.add(idx);
		//					} else if (originalSelectList.get(idx).toString().contains("SUM(") && originalSelectList.toString()
		//							.contains(originalSelectList.get(idx).toString().replace("SUM(", "AVG("))) {
		//					} else if (originalSelectList.get(idx).toString().contains("SUM(") && originalSelectList.toString()
		//							.contains(originalSelectList.get(idx).toString().replace("SUM(", "COUNT("))) {
		//					} else if (originalSelectList.get(idx).toString().contains("COUNT(") && originalSelectList
		//							.toString().contains(originalSelectList.get(idx).toString().replace("COUNT(", "SUM("))) {
		//						indicesToRemove.add(idx);
		//					} else if (originalSelectList.get(idx).toString().contains("COUNT(*)")) {
		//						indicesToRemove.add(idx);
		//					}
		//				}
		//			}
		//		}

		Collections.sort(indicesToRemove, Collections.reverseOrder());
		for (int i : indicesToRemove) {
			selectlist.remove(i);
		}

		boolean alreadyPrinted = false;
		boolean simplePrint = false;
		int index = 0;
		int groupRowCount = 0;
		String temp = "";
		long lineCounter = 0;
		String groupKey = "";
		if (isStarPresent && whereclauseabsent) {
			sb.append(String.join(System.getProperty("line.separator"),
					Files.readAllLines(Paths.get(csvFile_local_copy))));
		} else if (whereclauseabsent && !isAggregate) {
			lineCounter = 0;
			while ((line = br.readLine()) != null) {
				lineCounter++;
				rowData = line.split("\\|", -1);
				for (int i = 0; i < selectItems.size(); i++) {
					result = e.eval(((SelectExpressionItem) selectItems.get(i)).getExpression());
					sb.append(result.toRawString().concat("|"));
				}
				sb.setLength(sb.length() - 1);
				sb.append("\n");
				if (lineCounter == plain.getLimit().getRowCount())
					break;
			}
		} else if (isStarPresent && !whereclauseabsent) {
			lineCounter = 0;
			while ((line = br.readLine()) != null) {

				rowData = line.split("\\|", -1);
				if (e.eval(whereExpression).toBool()) {
					lineCounter++;
					sb.append(line);
					sb.append("\n");
					if (plain.getLimit() != null && lineCounter == plain.getLimit().getRowCount())
						break;
				}

			}
		} else if (isAggregate && (groupByColumns == null) && !(primaryKeyToDataMap.size()>0 || indexToPrmryKeyMap.size()>0 )) {// if no group by
			// but aggregate
			// is present //
			// old code

			// Thread m = new
			// MyThread2(null,tableName,whereExpression,columnDataTypes,columnNameToIndexMapping,
			// plain);
			// m.start();
			//
			// Thread m2 = new
			// MyThread2(tableName,tableName+"2",whereExpression,columnDataTypes,columnNameToIndexMapping,
			// plain);
			// m2.start();

			Aggregate a = new Aggregate();
			a.setColumnDataTypes(columnDataTypes);
			a.setColumnNameToIndexMapping(columnNameToIndexMapping);
			a.setPlain(plain);
			a.getSelectedColumns(tableName, whereExpression);
			// Aggregate.getSelectedColumns(tableName+"2", whereExpression);
			simplePrint = true;
			return;
		}
		else if((primaryKeyToDataMap.size()>0 || indexToPrmryKeyMap.size()>0) && (groupByColumns==null)){
			
			simplePrint = true;
			int lineNum = 0;
			String line32=""; 			
			if(whereclauseabsent){
				simplePrint = false;
				alreadyPrinted = true;
				Aggregate a = new Aggregate();
				a.setColumnDataTypes(columnDataTypes);
				a.setColumnNameToIndexMapping(columnNameToIndexMapping);
				a.setPlain(plain);
				a.getSelectedColumns(tableName, whereExpression);
				// Aggregate.getSelectedColumns(tableName+"2", whereExpression);
			}
			else if(indexToPrmryKeyMap.size()>0){
				if(isAggregate){
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
					//for loop ends

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



					avoidDuplicates = new ArrayList<String> ();
					l = new ArrayList<Expression>();

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
							if(l.get(idx).toString().contains("(*)")){
								l.remove(idx);
								break;
							}
						}
					}
					selectlist = l;
				}
				//Is Aggregate check complete!
				
				
				if(((AndExpression)whereExpression)!=null){

					//int expCount = whereExpression.toString().split("AND").length;
					Expression a = ((AndExpression)whereExpression);
					while(((AndExpression)a).getLeftExpression() instanceof AndExpression){
						//if((AndExpression)a).getLeftExpression() instanceof (AndExpression))
						a = (AndExpression)((AndExpression)a).getLeftExpression();
					}
					//indexToPrmryKeyMap.keySet().toArray(a)

					rowData = new String[columnNameToIndexMapping.get(tableName).size()];

					String final_s=null;
					for(String s: indexNameToColumnsMap.keySet())
					{
						if(s.contains("INDEX"))
						{
							
							if(a.toString().split(((AndExpression)a).getStringExpression())[0].contains(indexNameToColumnsMap.get(s).get(0).toUpperCase())
									&& a.toString().split(((AndExpression)a).getStringExpression())[1].contains(indexNameToColumnsMap.get(s).get(0).toUpperCase())){
								System.out.println("both expressions contain index column");
								final_s=s;
								break;
							}
							else if(a.toString().split(((AndExpression)a).getStringExpression())[0].contains(indexNameToColumnsMap.get(s).get(0).toUpperCase())){
								System.out.println("left expression contain index column");
								a = ((AndExpression)a).getLeftExpression();
								final_s=s;
								break;
							}
							else if(a.toString().split(((AndExpression)a).getStringExpression())[1].contains(indexNameToColumnsMap.get(s).get(0).toUpperCase())){
								System.out.println("right expression contain index column");
								a = ((AndExpression)a).getRightExpression();
								final_s=s;
								break;
							}		
						}
					}
					if ((final_s != null)
							&& (a.toString().contains(indexNameToColumnsMap.get(final_s).get(0).toUpperCase()))) {
						for(Object o: indexToPrmryKeyMap.keySet()){
							
							rowData[columnNameToIndexMapping.get(tableName).get(indexNameToColumnsMap.get(final_s).get(0).toUpperCase())] = o.toString();

							if(e.eval(a).toBool()){
								List<Object> li = indexToPrmryKeyMap.get(o);
								for(Object data: li){		
									count_All++;
									rowData = (String[])primaryKeyToDataMap.get(data);
									//									try (Stream<String> lines = Files.lines(Paths.get(csvFile_local_copy))) {
									//										    rowData = lines.skip(lineNum).findFirst().get().split("\\|",-1);
									//										    //System.out.println(line32);
									//										}
									if(e.eval(whereExpression).toBool()){
										
										//System.out.println(rowData);
										if (!isAggregate) //If there is no Aggregate Statement in Query
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
														//test.add(e.eval(operand).toDouble());
														//innerCount++;
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
																aggrMap[i]+=1;
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
					}
					else{
						System.out.println("Where does not contain any indexed column but has aggregate selectitems!");
						simplePrint = false;
						alreadyPrinted = true;
						Aggregate agg = new Aggregate();
						agg.setColumnDataTypes(columnDataTypes);
						agg.setColumnNameToIndexMapping(columnNameToIndexMapping);
						agg.setPlain(plain);
						agg.getSelectedColumns(tableName, whereExpression);
					}
				}
			}
			else{

			}
		}
		else {// if group by is present and where is present
			lineCounter = 0;
			// Timer t = new Timer();
			Date d = new Date();
			int hrs = d.getHours();
			int min = d.getMinutes();
			int sec = d.getSeconds();
			while ((line = br.readLine()) != null) {
				groupKey = "";
				rowData = line.split("\\|", -1);
				if (e.eval(whereExpression).toBool()) {
					lineCounter++;
					// System.out.println(lineCounter);
					if (groupByColumns != null) {
						for (int i = 0; i < groupByColumns.size() - 1; i++) {
							groupKey += e.eval(groupByColumns.get(i)).toRawString() + "|";
						}
						groupKey += e.eval(groupByColumns.get(groupByColumns.size() - 1)).toRawString();
						if (groupByMap.get(groupKey) == null) {
							groupByMapDenominators.put(groupKey, new ArrayList<Integer>());
							groupByMap.put(groupKey, new ArrayList<Double>());
							groupByStringMap.put(groupKey, new ArrayList<String>());
							for (int i = 0; i < selectlist.size(); i++) {
								groupByMap.get(groupKey).add(0.0);
								groupByMapDenominators.get(groupKey).add(0);
								groupByStringMap.get(groupKey).add("");
							}
						}
					}
					// Key Should be generated according to group order by

					// if(e.eval(((SelectExpressionItem)selectItems.get(i)).getExpression()))
					if (!isAggregate && (groupByColumns == null)) {
						for (int i = 0; i < selectItems.size(); i++) {

							result = e.eval(((SelectExpressionItem) selectItems.get(i)).getExpression());
							sb.append(result.toRawString().concat("|"));
						}
						sb.setLength(sb.length() - 1);
						sb.append("\n");
						if (plain.getLimit() != null && lineCounter == plain.getLimit().getRowCount())
							break;
					} else {

						for (int i = 0; i < selectlist.size(); i++) {
							Function func = null;
							if (selectlist.get(i) instanceof Function) {
								func = (Function) selectlist.get(i);
								switch (func.getName().charAt(2)) {
								case 'G':// AVG
									result = e.eval(func.getParameters().getExpressions().get(0));
									if (result != null) {
										groupByMap.get(groupKey).set(i,
												groupByMap.get(groupKey).get(i) + result.toDouble());
										groupByMapDenominators.get(groupKey).set(i,
												groupByMapDenominators.get(groupKey).get(i) + 1);
									}
									break;
								case 'M':// SUM
									result = e.eval(func.getParameters().getExpressions().get(0));
									if (result != null) {
										groupByMap.get(groupKey).set(i,
												groupByMap.get(groupKey).get(i) + result.toDouble());
									}
									break;
								case 'U':// COUNT
									if (func.toString().contains("(*)")) {
										groupByMap.get(groupKey).set(i, groupByMap.get(groupKey).get(i) + 1);
									} else {
										operand = (Expression) func.getParameters().getExpressions().get(0);
										result = e.eval(operand);
										// index
										// =columnNameToIndexMapping.get(tableName).get(operand.toString());
										temp = result.toRawString();// record.get(index);//(rowData[index]);
										if (temp.trim().length() != 0) {
											// result = e.eval(operand);
											if (e.eval(operand) != null) {
												groupByMap.get(groupKey).set(i, groupByMap.get(groupKey).get(i) + 1);
											}
										}
									}
									break;
								case 'N':// MIN
									operand = (Expression) func.getParameters().getExpressions().get(0);
									result = e.eval(operand);
									// index
									// =columnNameToIndexMapping.get(tableName).get(operand.toString());
									temp = result.toRawString();// record.get(index);//(rowData[index]);
									if (temp.trim().length() != 0) {
										// result = e.eval(operand);
										if (result.toDouble() < groupByMap.get(groupKey).get(i)) {
											groupByMap.get(groupKey).set(i, result.toDouble());
										}
									}
									break;
								case 'X':// MAX
									operand = (Expression) func.getParameters().getExpressions().get(0);
									result = e.eval(operand);
									// index
									// =columnNameToIndexMapping.get(tableName).get(operand.toString());
									temp = result.toRawString();// record.get(index);//(rowData[index]);
									if (temp.trim().length() != 0) {
										// result = e.eval(operand);
										if (result.toDouble() > groupByMap.get(groupKey).get(i)) {
											groupByMap.get(groupKey).set(i, result.toDouble());
										}
									}
									break;
								default:
									break;
								}

							} else {// If the Column is not an aggregate column
								// then simply get the value
								operand = selectlist.get(i);
								if (groupByStringMap.get(groupKey).get(i).trim().length() == 0) {
									groupByStringMap.get(groupKey).set(i, e.eval(operand).toRawString());
								}
							}
						}
					}

					// result =
					// e.eval(((SelectExpressionItem)selectItems.get(selectItems.size()-1)).getExpression());
					// sb.append(result.toRawString()+"\n");

				}
			}
			d = new Date();
			int hrs1 = d.getHours();
			int min1 = d.getMinutes();
			int sec1 = d.getSeconds();
			// System.out.println("time to execute gropu by = "+
			// (hrs1-hrs)+":"+(min1-min)+":"+(sec1-sec));
		}

		if(!alreadyPrinted && isAggregate && (groupByColumns==null))
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
						index = 0 ;
						if(columnNameToIndexMapping.get(tableName).get(columnName)==null){
							index = -1;
							sb.append( aggregateHistory.get(tableName).get(columnName).get(item.getName()) + "|");
						}
						else {
							index = columnNameToIndexMapping.get(tableName).get(columnName);
							String type = columnDataTypes.get(tableName).get(index);
							Double tempVal= aggregateHistory.get(tableName).get(columnName).get(item.getName());
							sb.append((type.equals("long")||type.equals("int"))? tempVal.longValue()+"|":temp+"|");
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
			sb.setLength(sb.length()-1);
		}

		if (!alreadyPrinted && isAggregate && !simplePrint) {
			// for orderby implement logic // later
			List<String> keyList = new ArrayList<String>(groupByMap.keySet());
			Collections.sort(keyList);
			//
			for (String outputGroupKey : keyList) {

				for (int i = 0; i < selectlist.size(); i++) {
					item = null;
					if (selectlist.get(i) instanceof Function) {
						item = (Function) selectlist.get(i);
						switch (item.getName()) {
						case "SUM":
						case "MIN":
						case "MAX":
							sb.append((result instanceof LongValue)
									? ((groupByMap.get(outputGroupKey).get(i).longValue()) + "|")
											: groupByMap.get(outputGroupKey).get(i) + "|");
							break;
						case "count":
						case "COUNT":
							sb.append(groupByMap.get(outputGroupKey).get(i).longValue() + "|");
							break;
						case "AVG":
							sb.append((groupByMap.get(outputGroupKey).get(i)
									/ groupByMapDenominators.get(outputGroupKey).get(i)) + "|");
							break;
						default:
							break;
						}
					} else {// if its a Simple String Column or Simple Number
						// Column
						sb.append(groupByStringMap.get(outputGroupKey).get(i) + "|");
					}
				}
				sb.setLength(sb.length() - 1);
				sb.append("\n");
			}
			if (sb.length() >= 2) {

				sb.setLength(sb.length() - 1);
				sb.append("\n");
			}
		} else {
			if (orderByElements != null && orderByElements.size() > 0) {
				orderElements();
			}
		}
		// System.out.println(groupByMap);
		// sb.setLength(sb.length() - 1);
		System.out.println(sb.toString());// to print normal queries
	}

	public void orderElements() {

	}

	public static void main(String[] args) throws Exception {

		SortedMap<String, String> sm = new TreeMap<String, String>();
		sm.put("xaviers".toLowerCase(), "colege");
		sm.put("ABUZAR".toLowerCase(), "boy");
		sm.put("abuzar".toLowerCase(), "boy");
		sm.put("University at Buffalo".toLowerCase(), "school");
		sm.put("ub".toLowerCase(), "boy");

		Main main = new Main();

		System.out.print("$>");
		scan = new Scanner(System.in);
		String temp;
		String query = "";
		while ((temp = scan.nextLine()) != null) {
			query += temp + " ";
			if (temp.indexOf(';') >= 0) {
				main.readQueries(query);
				main.parseQueries();
				System.out.print("$>");
				query = "";
			}

		}
		scan.close();

	}

	class EvalLib extends Eval {
		String tableName = "";
		boolean isIndexed = false;
		public EvalLib(String tableName) {
			this.tableName = tableName;
		}

		@Override
		public PrimitiveValue eval(Column col) throws SQLException {
			if(!isIndexed){
				int index = columnNameToIndexMapping.get(tableName).get(col.getColumnName());
				switch (columnDataTypes.get(tableName).get(index).toLowerCase()) {
				case "string":
				case "varchar":
				case "char":
					return new StringValue(rowData[index]);
					// return new StringValue(record.get(index));
				case "int":
					return new LongValue(rowData[index]);
					// return new LongValue(record.get(index));
				case "decimal":
					return new DoubleValue(rowData[index]);
					// return new DoubleValue(record.get(index));
				case "date":
					return new DateValue(rowData[index]);
					// return new DateValue(record.get(index));
				default:
					return new StringValue(rowData[index]);
					// return new StringValue(record.get(index));
				}
			}
			else{
				if(indexNameToColumnsMap.get("INDEX").get(0).contains(col.getColumnName())){
					//indexToPrmryKeyMap.keySet().toArray(a);

				}
				return new StringValue("");
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
	private Thread t = null;

	public String tableName = "";
	public Expression whereExpression = null;
	public String originalTable = "";
	public PlainSelect plain = null;
	public Map<String, Map<String, Integer>> colNameToIndexMapping = null;
	public Map<String, ArrayList<String>> colDataTypes = null;

	public void start() {
		if (t == null) {
			t = new Thread(this, tableName);
			t.start();
		}

	}

	public MyThread2(String originalTable, String tableName, Expression whereExpression,
			Map<String, ArrayList<String>> colDataTypes, Map<String, Map<String, Integer>> colNameToIndexMapping,
			PlainSelect plain) {
		super("MyThread2");
		this.tableName = tableName;
		this.whereExpression = whereExpression;
		this.originalTable = originalTable;
		this.colDataTypes = colDataTypes;
		this.colNameToIndexMapping = colNameToIndexMapping;
		this.plain = plain;
	}

	public void run() {
		try {

			if (originalTable != null) {
				Date d = new Date();
				long t1 = d.getTime();

				System.out.println("-------T1-----");
				// for(int i=0;i<100;i++){
				// System.out.println("T1->"+i);
				// Thread.sleep(50);
				// }
				final ThreadLocal<Aggregate> threadId = new ThreadLocal<Aggregate>() {
					@Override
					protected Aggregate initialValue() {
						return new Aggregate();
					}
				};
				try {
					Map<String, ArrayList<String>> a2MapDT = new HashMap<String, ArrayList<String>>();
					a2MapDT.put(tableName, colDataTypes.get(originalTable));
					Map<String, Map<String, Integer>> a2MapIndex = new HashMap<String, Map<String, Integer>>();
					a2MapIndex.put(tableName, colNameToIndexMapping.get(originalTable));

					threadId.get().setColumnNameToIndexMapping(a2MapIndex);
					threadId.get().setColumnDataTypes(a2MapDT);
					threadId.get().setPlain(plain);
					threadId.get().getSelectedColumns(tableName, whereExpression);

				} catch (Exception ex) {
					ex.printStackTrace();
				}
				d = new Date();
				long t2 = d.getTime();
				System.out.println(tableName + "->Time taken->" + (t2 - t1));
			}
			// http://www.tutorialspoint.com/java/java_multithreading.htm
			else {
				Date d = new Date();
				long t1 = d.getTime();
				try {
					System.out.println("-------T2-----");
					// for(int i=0;i<100;i++){
					// System.out.println("T2->"+i);
					// Thread.sleep(50);
					// }

					final ThreadLocal<Aggregate> threadId = new ThreadLocal<Aggregate>() {
						@Override
						protected Aggregate initialValue() {
							return new Aggregate();
						}
					};

					threadId.get().setColumnDataTypes(colDataTypes);
					threadId.get().setColumnNameToIndexMapping(colNameToIndexMapping);
					threadId.get().setPlain(plain);
					threadId.get().getSelectedColumns(tableName, whereExpression);
					// threadId.get().func2(originalTable,tableName,
					// whereExpression);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				d = new Date();
				long t2 = d.getTime();
				System.out.println(tableName + "->Time taken->" + (t2 - t1));
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	// public static void setTableName(String tableName) {
	// MyThread1.tableName = tableName;
	// }
	// public static void setWhereExpression(Expression whereExpression) {
	// MyThread1.whereExpression = whereExpression;
	// }

}

// class MyThread1 extends Thread {
// public static String tableName = "";
// public static String originalTable = "";
// public static Expression whereExpression = null;
// public static PlainSelect plain= null;
// public static Map<String,Map<String,Integer>> colNameToIndexMapping = null;
// public static Map<String, ArrayList<String>> colDataTypes = null;
// public MyThread1(String originalTable, String tableName,Expression
// whereExpression
// , Map<String, ArrayList<String>> colDataTypes
// ,Map<String,Map<String,Integer>> colNameToIndexMapping
// ,PlainSelect plain) {
// super("MyThread1");
// MyThread1.tableName = tableName;
// MyThread1.whereExpression = whereExpression;
// MyThread1.originalTable = originalTable;
// MyThread1.colDataTypes = colDataTypes;
// MyThread1.colNameToIndexMapping =colNameToIndexMapping;
// MyThread1.plain = plain;
// }
// public void run() {
//
// }
// // public static void setTableName(String tableName) {
// // MyThread1.tableName = tableName;
// // }
// // public static void setWhereExpression(Expression whereExpression) {
// // MyThread1.whereExpression = whereExpression;
// // }
//
// }
