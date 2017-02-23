
package project1;
import java.io.StringReader;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Scanner;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.WithItem;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;



public class Main{

	public enum data_types  {String,varchar,Char,Int,decimal,date}; 
    public static void main(String[] args) throws Exception
    {

        Scanner scan = new Scanner(System.in);
        String temp = scan.nextLine();
      
        scan.close();
        //System.out.println(temp);
        StringReader input = new StringReader(temp);
        CCJSqlParser parser = new CCJSqlParser(input);
        Statement statement = parser.Statement();
        Map<String,Integer> columnName = null;
        String currentDir = System.getProperty("user.dir");
        
        System.out.println(currentDir);
        String csvFile = "src\\project1\\data\\";
        BufferedReader br = null;
        String line = "";
        
        ArrayList <String> data_type = new ArrayList <String>();

        while(statement != null)
        {

            if(statement instanceof Select){
            	
            	
//            	Iterator it = s.listIterator();
//            	while(it.hasNext()){
//            		System.out.println(it.next());
//            	}
                Select select = (Select)statement;
                
                SelectBody body = select.getSelectBody();
               
                
                if(body instanceof PlainSelect){
                    PlainSelect plain = (PlainSelect)body;
                    List<SelectItem> si =  plain.getSelectItems();
                    ListIterator<SelectItem> it = si.listIterator();
                    int col[] = new int[si.size()] ;
                    int i=0;
                    //column name
                    while(it.hasNext()){
                    	String name = (String) it.next().toString();
                    	System.out.println(name);
                    	System.out.println(columnName.get(name));
                    	col[i]=columnName.get(name);
                    	i++;
                    }
//                    BinaryExpression ex   =  (BinaryExpression)plain.getWhere();
//                    System.out.println(
//                   ex.getLeftExpression()+" , "+
//                   ex.getRightExpression()+" , "+
//                   ex.getStringExpression());
                    //Expression ex = ((PlainSelect) body).getWhere().accept();;
                    
                    
                    Table table = (Table) plain.getFromItem();
                    String tableName = table.getName();
                   
                    csvFile = csvFile+tableName+".csv";
                    //System.out.println(csvFile);
                    br = new BufferedReader(new FileReader(String.format(csvFile)));
                    while ((line = br.readLine()) != null) {

                        // use comma as separator
                        String[] Columns = line.split("\\|");

                        for(i=0;i<col.length-1;i++)
                        {
                            System.out.print(Columns[col[i]]+"|");
                        }
                        System.out.print(Columns[col[col.length-1]]+"\n");
                        //System.out.println();
                    }
                    

                }

                else {
                    throw new Exception("I can't understand body instanceof PlainSelect "+statement);
                }
                /** Do something with the select operator **/
            } 
            else if(statement instanceof CreateTable){
            	CreateTable create = (CreateTable)statement;
            	columnName = new HashMap<String,Integer>();
            	List<ColumnDefinition> si = create.getColumnDefinitions();
            	ListIterator<ColumnDefinition> it = si.listIterator();
            	
            	int i=0;
                while(it.hasNext()){
                	//System.out.println(it.next());
                	
                	ColumnDefinition cd = it.next();
                	data_type.add(cd.getColDataType().toString());
                	System.out.println("type = "+ data_type.get(i));
                	columnName.put(cd.getColumnName() ,i++);
                	
            	
                }
                
            }
            else {
                throw new Exception("I can't understand statement instanceof Select"+statement);
            }
            statement = parser.Statement();
        }


    }
    
    


}