package dubstep;
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
			else
			{
				return new StringValue(rowData[index].trim());
			}
		}
	}
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

			/*
			import org.apache.commons.csv.CSVFormat;
			import org.apache.commons.csv.CSVParser;
			import org.apache.commons.csv.CSVRecord;

			Reader in = new FileReader(file);
						CSVParser parser = new CSVParser(in, CSVFormat.EXCEL.withHeader(tblschema.getHeaders()).withDelimiter('|'));
			 */
			List<SelectItem> SelectStatements = plain.getSelectItems();
			ArrayList <Function> selectlist = new ArrayList<Function>();
			boolean is_aggregate=false;
			for(SelectItem select: SelectStatements)
			{
				//System.out.println(select);
				Expression expression = ((SelectExpressionItem)select).getExpression();
				if(expression instanceof Function)
				{
					is_aggregate = true;
					selectlist.add((Function) expression);
				}
			}


			//System.out.println("is_aggregate = " + is_aggregate);
			PrimitiveValue eval_result = null;
			PrimitiveValue Max = null;
			PrimitiveValue Min = null;
			int countAll =0;
			int countNonNull =0;
			double total=0;
			double sum=0;
			int row_count=0;
			double average=0;
			
			boolean whereclauseabsent = (plain.getWhere()==null)?true:false;
			
			while ((line = br.readLine()) != null) 
			{
				//System.out.println("Debug: "+line);
				String Line1 = line.replace("|", "| ");
				rowData = Line1.split("\\|");
				if(whereclauseabsent || e.eval(whereExpression).toBool())
				{
					if (is_aggregate ==false)
					{
						for(int i=0;i<SelectStatements.size();i++)
						{
							PrimitiveValue result = e.eval(((SelectExpressionItem)SelectStatements.get(i)).getExpression());
							sb.append(result+"|");
						}

						if(sb.length() >=2)
						{
							sb.setLength(sb.length() - 1);
							sb.append("\n");
						}
						//PrimitiveValue result = e.eval(((SelectExpressionItem)SelectStatements.get(SelectStatements.size()-1)).getExpression());
						//sb.append(result+"\n");
					}
					else
					{
						for(int i =0; i<selectlist.size();i++)
						{
							Function item = selectlist.get(i);
							if(item.getName().equalsIgnoreCase("SUM"))
							{
								Expression operand = (Expression) item.getParameters().getExpressions().get(0);
								PrimitiveValue result = e.eval(operand);
								sum+=result.toDouble();
								
							}
							else if(item.getName().equalsIgnoreCase("AVG"))
							{
								Expression operand = (Expression) item.getParameters().getExpressions().get(0);
								PrimitiveValue result = e.eval(operand);
								total += result.toDouble();
								row_count++;
								average=total/row_count;
							}
							else if(item.toString().toLowerCase().contains("count(*)"))
							{
								countAll++;
							}
							else if(item.getName().equalsIgnoreCase("COUNT"))
							{
								Expression operand = (Expression) item.getParameters().getExpressions().get(0);
								int index =columnNameToIndexMapping.get(tableName).get(operand.toString());
								String temp =(rowData[index].trim());
								if(temp.length() != 0)
								{
									eval_result = e.eval(operand);
									if(eval_result!=null)
									{
										countNonNull++;
									}
								}
							}
							else if(item.getName().equalsIgnoreCase("MIN"))
							{
								Expression operand = (Expression) item.getParameters().getExpressions().get(0);
								eval_result = e.eval(operand);
								if(Min==null)
								{
									Min = eval_result;
								}
								else if(eval_result.toDouble() < Min.toDouble())
								{
									Min = eval_result;
								}
							}
							else if(item.getName().equalsIgnoreCase("MAX"))
							{
								Expression operand = (Expression) item.getParameters().getExpressions().get(0);
								eval_result = e.eval(operand);
								if(Max==null)
								{
									Max = eval_result;
								}

								else if(eval_result.toDouble() > Max.toDouble())
								{
									Max = eval_result;
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
					Function item = selectlist.get(i);
					if(item.getName().equalsIgnoreCase("SUM"))
					{
						sb.append(sum+"|");
					}
					else if(item.getName().equalsIgnoreCase("AVG"))
					{
						sb.append(average+"|");
					}
					else if(item.toString().toLowerCase().contains("count(*)"))
					{
						sb.append(countAll+"|");
					}
					else if(item.getName().equalsIgnoreCase("COUNT"))
					{
						sb.append(countNonNull+"|");
					}
					else if(item.getName().equalsIgnoreCase("Min"))
					{
						sb.append(Min+"|");
					}
					else if(item.getName().equalsIgnoreCase("Max"))
					{
						sb.append(Max+"|");
					}
				}
			}
			if(sb.length() >=2)
			{
				sb.setLength(sb.length() - 1);
				sb.append("\n");
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