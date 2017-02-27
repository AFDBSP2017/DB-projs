
package project1;
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
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExpressionVisitorBase;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
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
import net.sf.jsqlparser.statement.select.SelectItem;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import net.sf.jsqlparser.eval.*;
class Evaluate extends Eval{

	@Override
	public PrimitiveValue eval(Column arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	
//	public Evaluate () throws SQLException{
//		this.
//	}
}

class Evallib extends Eval
{
	public Evallib(ColumnDefinition cd) throws SQLException {

		//leila 
		Map<String,Integer> query = new HashMap<String,Integer>();
		query.put("A",1);
		query.put("B",2);
		query.put("C",3);
		query.put("D",4);
		String str= "A+B-C+D";
		//String colName = cd.getColumnName();
		PrimitiveValue result;
		String strt=str;

		double sum=query.get(strt.charAt(0)+"");
		int i=1;
		while (i!=strt.length()){
			if(strt.charAt(i)=='+'){
				sum+=query.get(strt.charAt(i+1)+""); i+=2;
			}
			else if(strt.charAt(i)=='-'){
				sum-= query.get(strt.charAt(i+1)+""); i+=2;
			}
		}
		System.out.println("Sum= "+ sum);
		result = 
				this.eval(
						new Addition(
								new LongValue(1),
								new DoubleValue(2.0)
								)
						); 
		System.out.println("Result: "+result); // "Result: 3.0"

		// Evaluate "1 > (3.0 * 2)"--leila
		result = 
				this.eval(
						new GreaterThan(
								new LongValue(8),
								new Multiplication(
										new DoubleValue(3.0),
										new LongValue(2)
										)
								)
						);
		System.out.println("Result: "+result); // "Result: false"
	} /* we'll get what goes here shortly */

	@Override
	public PrimitiveValue eval(Column arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	} 

}

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

		Evallib e = new Evallib(new ColumnDefinition());
		//create table R(A int, B String, C String, D int ); select A,B,C,D from R
		//create table R(A int, B String, C String, D int ); select A,B from R where A=1 and B=1;select * from R
		//create table R(A int, B String, C String, D int ); select A,B from R where (A=1 and B=2) OR (B=1 AND D =9)
		//create table R(A int, B String, C String, D int ); select A,B from R where (A=1 and B=2) OR (C>4 AND B=1 AND D =9)
		//create table R(A int, B String, C String, D int ); select A,B from R where (A=1 and B=2) OR (C>4 AND B=1) AND (B>2 OR D =9)
		System.out.println("Hello");
		System.out.print("$> ");
		scan = new Scanner(System.in);
		String temp;
		//= scan.nextLine();

		while((temp = scan.nextLine()) != null)
		{
			readQueries(temp);
			parseQueries();
			//Evallib e = new Evallib();
			System.out.print("$> ");
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

		select = (Select)statement;
		body = select.getSelectBody();


		if(body instanceof PlainSelect){

			findcolumnsToFetchInSelectStatement();
			getSelectiveColumnsAsPerSelectStatement();

			if (plain.getWhere() != null) 
			{
				System.out.println("plain expression  " + plain.getWhere());
				String expression =plain.getWhere().toString();
				String str1 = expression.replaceAll("AND", "&&");
				String str2 = "(" + str1.replaceAll("OR", "||") + ")";
				//getWhereConditionList(str2);
				String te = checkLeftRightExpressions((BinaryExpression)plain.getWhere());

			}

		}

		else {
			throw new Exception("I can't understand body instanceof PlainSelect "+statement);
		}
		/** Do something with the select operator **/

	}
	
	
	public static String checkLeftRightExpressions(BinaryExpression ex){
		String l = null,r = null,o = null;
		List<Expression> el=null;
		if(ex instanceof OrExpression || ex instanceof AndExpression)
			el = ExpressionVisitorBase.getChildren(ex);
		else 
			el = null;
		if(el!=null){
			checkLeftRightExpressions((BinaryExpression)el.get(0));
			checkLeftRightExpressions((BinaryExpression)el.get(1));
		}
		else{
				l = ((BinaryExpression) ex).getLeftExpression() + "";
				r = ((BinaryExpression) ex).getRightExpression() + "";
				o = ((BinaryExpression) ex).getStringExpression();
		}
		if(l==null||o==null||r==null){
			if(ex instanceof AndExpression){
				System.out.println("&&");
			}
			else{
				System.out.println("||");
			}
		}
		else{
			System.out.println("("+l+o+r+")");
		}
		return "("+l+o+r+")";
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
		System.out.println(plain.getFromItem());
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
		//System.out.println("Size  = " +columnIndexesToFetchInSelectStatement.length);
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


	public static void getColumnDataTypesAndMapColumnNameToIndex() throws SQLException
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
			Evallib e = new Evallib(cd);
			columnDataTypes.add(cd.getColDataType().toString());
			System.out.println("type = "+ columnDataTypes.get(i));
			columnNameToIndexMap.put(cd.getColumnName() ,i++);
		}
	}


	public static void readQueries(String temp) throws ParseException
	{

		StringReader input = new StringReader(temp);
		parser = new CCJSqlParser(input);

		statement = parser.Statement();    
	}
	
	
	// Returns true if 'op2' has higher or same precedence as 'op1',
	// otherwise returns false.
	public static boolean hasPrecedence(String op1, String op2)
	{
		if (op2.equals("(") || op2.equals(")"))
			return false;
		if ((op1.equals("*") || op1.equals("/")) && (op2.equals("+")|| op2.equals("-")))
			return false;
		else
			return true;
	}
	
	public static void getWhereConditionList(String Expression) throws IOException
	{
		//postfix notation
		//http://introcs.cs.princeton.edu/java/43stack/Evaluate.java.html
		//http://codinghelmet.com/?path=exercises/expression-evaluator
		//http://www.geeksforgeeks.org/expression-evaluation/


		System.out.println("After replacing  " + Expression);
		System.out.println("length = " + Expression.length());

		int index =0;
		Stack<String> ops  = new Stack<String>();
		Stack<String> vals = new Stack<String>();
		String stack_expression=null;
		boolean break_flag=false;

		while (index < Expression.length()) {
			String s = String.valueOf(Expression.charAt(index));
			//System.out.println("s : "+ s);
			if(s.equals(" "))
			{
				index++;
				continue;
			}
			else if  (s.equals("("))
			{
				ops.push("(");
				System.out.println("operator : (");
			}
			else if (s.equals("|"))
			{

				if((index < Expression.length() -1) && (String.valueOf(Expression.charAt(index+1)).equals("|")))
				{
					index++;
					index++;
					ops.push("||");
					System.out.println("operator : ||");
					continue;
				}
				ops.push(s);
				System.out.println("operator : |");
			}
			else if (s.equals("&"))
			{
				if((index < Expression.length() -1) && (String.valueOf(Expression.charAt(index+1)).equals("&")))
				{
					index++;
					index++;
					ops.push("&&");
					System.out.println("operator : &&");
					continue;
				}
				ops.push(s);
				System.out.println("operator : &");
			}
			else if (s.equals(")")) 
			{
				System.out.println("operator : )");

				while (!ops.peek().equals("("))
				{
					stack_expression = vals.pop().toString() +  ops.pop() + vals.pop();
					vals.push(stack_expression);

				}
				ops.pop();
				/*
                if(op.equals("+"))
                {
                	stack_expression += vals.pop().toString() +  "+" + v;
                }
                else if (op.equals("-"))
                {
                	stack_expression += vals.pop().toString() +  "-" + v;
                }
                else if (op.equals("*"))
                {
                	stack_expression += vals.pop().toString() +  "*" + v;
                }
                else if (op.equals("/"))
                {
                	stack_expression += vals.pop().toString() +  "/" + v;
                }
				 */

			}
			else if (IsOperator(s)==true)
			{
				// While top of 'ops' has same or greater precedence to current
				// token, which is an operator. Apply operator on top of 'ops'
				// to top two elements in values stack
				while (!ops.empty() && hasPrecedence(s, ops.peek()))
				{
					stack_expression = vals.pop().toString() +  ops.pop() + vals.pop();
					vals.push(stack_expression);
				}
				System.out.println("operator push : "+ s);
				ops.push(s);

			}
			else
			{
				int startindex= index;

				int endindex = startindex;
				String nextchar = String.valueOf(Expression.charAt(endindex+1));
				//System.out.println("startindex  : "+ startindex + "   nextchar : "+ nextchar);
				while((endindex < Expression.length()) && (IsOperator(nextchar)==false))
				{

					if(endindex == Expression.length()-1)
					{
						break;
					}
					endindex++;
					//System.out.println("inside loop endindex = "+ endindex);
					nextchar = String.valueOf(Expression.charAt(endindex+1));
				}

				//System.out.println("startindex  : "+ startindex + "  " + Expression.charAt(startindex)+ "endindex : " + endindex);

				String operand;
				if(startindex == endindex)
				{
					operand = String.valueOf(Expression.charAt(startindex));  
				}
				else
				{
					operand = Expression.substring(startindex, endindex).trim();            	
				}
				System.out.println("operand to push: "+ operand);
				vals.push(operand.toString());
			}


			if(index!=0 && ops.isEmpty() && vals.isEmpty())
			{
				System.out.println("Break");
				break;
			}
			index++;
			if(break_flag==true)
			{
				break;
			}
		}

		while (!ops.empty())
		{
			stack_expression = vals.pop().toString() +  ops.pop() + vals.pop();
			vals.push(stack_expression);
		}

		stack_expression =vals.pop();

		//System.out.println(vals.pop());
		System.out.println(stack_expression);
	}
	
	public static boolean IsOperator(String s) throws IOException
	{

		//System.out.println("s : "+  s);
		if (s.equals("+") || (s.equals("-")) || (s.equals("*")) || (s.equals("/")) || (s.equals("|")) 
				|| (s.equals("&")) || (s.equals("==")) || (s.equals("!=")) || (s.equals("=")) || 
				s.equals(")") || s.equals("("))
		{
			//System.out.println("return true  : "+  s);
			return true;
		}
		//System.out.println("return false");
		return false;
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
