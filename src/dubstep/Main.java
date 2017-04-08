/*
CREATE TABLE BOLDLY(DARINGLY int, POACH int, WITHIN string, FRAYS decimal);Select DARINGLY,WITHIN FROM BOLDLY WHERE POACH=65;
CREATE TABLE CREAM(INSIDE int, DECOYS int, DURING string, DOUBT decimal);
CREATE TABLE EXPRESS(ASYMPTOTES int, INSTRUCTIONS int, BUSY string, PEACH decimal);
CREATE TABLE PLAY(LINEN int, BLUE int, PAST string, WATERS decimal);
Select DARINGLY,WITHIN FROM BOLDLY WHERE POACH=65;
 */
package dubstep;
//import net.sf.jsqlparser.eval.Eval;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
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
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

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
			Table table = (Table) plain.getFromItem();
			String tableName = table.getName();
			getSelectedColumns(tableName, plain.getWhere());

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
		//implement me

		EvalLib e = new EvalLib(tableName);
		String csvFile_local_copy = csvFile+tableName+".csv";
		br = new BufferedReader(new FileReader(String.format(csvFile_local_copy)));
		StringBuilder sb = new StringBuilder();	
		List<SelectItem> selectItems = plain.getSelectItems();
		boolean isStarPresent = false;
		for(SelectItem select: selectItems)
		{
			//System.out.println(select);

			if(select.toString().equals("*")){ 
				
				isStarPresent = true; break;
				
			}
		}
		PrimitiveValue result=null;
		boolean whereclauseabsent = (plain.getWhere()==null)?true:false;
		
		while((line=br.readLine())!=null)
		{
			//record = iter.next();
			//System.out.println("Debug: "+line);
			rowData = line.split("\\|",-1);
			if(isStarPresent && e.eval(whereExpression).toBool()){
				
				sb.append(line+"\n");
				
			}
			else if(whereclauseabsent || e.eval(whereExpression).toBool())
			{
				for(int i=0;i<selectItems.size()-1;i++)
				{
					result = e.eval(((SelectExpressionItem)selectItems.get(i)).getExpression());
					sb.append(result.toRawString().concat("|"));
				}
				result = e.eval(((SelectExpressionItem)selectItems.get(selectItems.size()-1)).getExpression());
				sb.append(result.toRawString()+"\n");
				
			}
		}
		System.out.println(sb.toString());
	}
	
	public static Expression inExpression(InExpression exp){
		
		
		return null;
		
	}
	
	public static void main(String[] args) throws Exception {
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

