import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.btree.BTree;
import jdbm.helper.IntegerComparator;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

 

public class DBMain {
	private static ArrayList<String> tableNames = new ArrayList<String>();
	
	private static String columns = "";
	private static HashMap<String,String> tables = new HashMap<String,String>();
	private static HashMap<String,String> insertMap = new HashMap<String,String>();
	private static Scanner reader ; 
	private static RecordManager recman ; 

	
public static void main(String[] args) {
	 
	try { 
	loadTable();
	String DATABASE = "filebaseddb";
	Properties props = new Properties(); 
	try {
	recman = RecordManagerFactory.createRecordManager(DATABASE, props);
	} catch (IOException e) {
	 
	System.out.println("Tree exception");
	}
	/*
	String DATABASE = "filebaseddb";
	String BTREE_NAME = "complaints_bt_pid";
	Properties props = new Properties(); 
	RecordManager recman ;
	try {
	recman = RecordManagerFactory.createRecordManager(DATABASE, props);
	BTree tree = BTree.createInstance( recman, new IntegerComparator());
	            recman.setNamedObject( BTREE_NAME, tree.getRecid() );
	            for(int i=0; i<=109999;i++){
	            	tree.insert(i,i,false);
	            }
	            recman.commit();
	}
	catch(Exception e){
	System.out.println("Exception!");
	}
	
	
	*/
	 
	
	System.out.println("Existing tables : "  + tables.keySet().toString());
	} catch (FileNotFoundException e) {
	 
	System.out.println("File not found!");
	}
	while(true){
	System.out.println("\n1. Create new table -  c "
	+ "\n2. List all tables - l"
	+ "\n3. Delete existing tables - d"
	+ "\n4. Run a query"
	+ "\n5. Exit - e");
	
	reader = new Scanner(System.in); 
	String s = reader.nextLine();
	 
	if(s.equals("e")){
	System.out.println("Exiting ... ");
	writeHashMaptoFile();
	break ; 
	}
	
	switch(s){
	case "c":
	createNewTable();
	break ; 
	
	case "r":
	runQuery(); 
	break ; 
	
	case "l":
	System.out.println(tables.keySet().toString());
	break ; 
	
	
	case "d":
	deleteTable(); 
	
	break ; 
	
	case "b":
	browseTable(); 
	 
	break ; 
	 
	default:
	System.out.println("Please enter a value from the options above.");
	break;
	
	}
	
	}
	System.out.println("Writing tables to file .. ");
	writeHashMaptoFile();
}
  
private static void runQuery() {
	
	System.out.println("Enter your query. Available tables -  " );
	System.out.println(tables.keySet().toString());
	String query = reader.nextLine().trim(); 
	
	 //System.out.println("Query received - " + query);
	 query = query.replace("\'", ""); 
	 long startTime = System.nanoTime();
	 
	if(query.contains("SELECT")||query.contains("select")||query.contains("Select")){
	selectQuery(query); 
	}
	
	
	else if(query.contains("UPDATE")||query.contains("update") ||query.contains("Update"))
	updateQuery(query); 
	else if(query.contains("DELETE")||query.contains("delete")||query.contains("Delete"))
	deleteQuery(query); 
	else if(query.contains("Insert")||query.contains("insert")||query.contains("Insert"))
		insertIntoTable(query);
	
	else System.out.println("Invalid Query at start");
	
	long endTime = System.nanoTime();
	
	long duration = (endTime - startTime)/1000000;
	
	System.out.println("\n\nQuery Time = " + duration + " ms.");
	 
}
 
private static void deleteQuery(String qs) {
	 System.out.println("Inside deleteQueryy");
	// check that query contains the single condition
	 String[] qs_array = qs.split("\\s");
	 if(qs.indexOf("AND")==-1 && qs.indexOf("OR")==-1 ){
		// check 1 , if qs-array length != 7, then qs is invalid
		 
		if(qs_array.length != 7){
			System.out.println("Query is invalid...Inside Delete Query");
			return ;
		}
		
		String table_name = qs_array[2];
		String col = qs_array[4];
		String val = qs_array[6];
		String condition_operator = qs_array[5];
		String file_name = table_name.concat(".json");
		String BTree_name = table_name.concat("_bt_pid");
		
		
		// Loading the table and the BTree
		File table_file = new File(file_name);
		long recid = -1 ;
		try {
			 recid = recman.getNamedObject(BTree_name);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Error in reading BTree inside delete Query");
			e.printStackTrace();
		}
		// Check2 if table is doesn't exit 
		if(!table_file.exists()){
			System.out.println("Table dosen't exist \n Debugging info inside delete function");
			return ;
		}
		// Check3, if table is empty
		if(recid == -1){
			System.out.println("Table is empty. \n Debugging info inside the delete function");
			return ;
		}
		
		// Iterating through json and Btree
		BTree tree = null;
		try {
			tree = BTree.load(recman, recid);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Error Loading the tree inside the delete function check 4");
			e.printStackTrace();
		}
		if(tree.size() == 0){
			System.out.println("Table is Empty.");
			return ;
		}
		// *****HACK******
		if(col.equals("pid_".concat(table_name))){
			System.out.println("Performing the Deletion operation using B+Tree");
			//System.out.println("Intial Size of BTree is "+tree.size());
				
		}
		//----------------------
		
		// Loading the table
		JSONParser parser = new JSONParser();
		JSONArray jArray ;
		JSONObject jObject ;
		ArrayList<Integer> temp = new ArrayList<Integer>(); 
		try {
			Object ob = parser.parse(new FileReader(file_name));
			jArray = (JSONArray)ob ;
			if(condition_operator.equals("=")){
				for(int i = 0 ; i<jArray.size() ; i++){
					jObject = (JSONObject)jArray.get(i);
					Set keyset = jObject.keySet() ;
					for(Object o : keyset){
						if(o.toString().toLowerCase().equals(col)){
							if(jObject.get(col).equals(val)){
								temp.add(i);
							}
						}
					}
				}
			}
			else if(condition_operator.equals("<")){
				for(int i = 0 ; i<jArray.size();i++){
					jObject = (JSONObject)jArray.get(i);
					Set keyset = jObject.keySet();
					for(Object o : keyset){
						if(o.toString().toLowerCase().equals(col)){
							try{
							Integer col_val = Integer.parseInt((String)jObject.get(col)) ;
							Integer input_val = Integer.parseInt(val) ;
							
							if(Integer.compare(col_val,input_val) < 0){
							temp.add(i);	
							}
						}
							catch(NumberFormatException e){
								System.out.println("Invalid attrbute to perform the query. \n Debugging inside the delete condition < ");
								e.printStackTrace();
							}
						}
					}
				}
			}
			
			
			else if(condition_operator.equals(">")){
				for(int i = 0 ; i<jArray.size();i++){
					jObject = (JSONObject)jArray.get(i);
					Set keyset = jObject.keySet();
					for(Object o : keyset){
						if(o.toString().toLowerCase().equals(col)){
							try{
							Integer col_val = Integer.parseInt((String)jObject.get(col)) ;
							Integer input_val = Integer.parseInt(val) ;
							
							if(Integer.compare(col_val,input_val) > 0){
							temp.add(i);	
							}
						}
							catch(NumberFormatException e){
								System.out.println("Invalid attrbute to perform the query. \n Debugging inside the delete condition < ");
								e.printStackTrace();
							}
						}
					}
				}
			}
			
			
				// Updating the json Array and file
				System.out.println("Intial Size of B+Tree "+tree.size());
				for(int i : temp){
					jArray.remove(i);
					jObject = new JSONObject();
					jArray.add(i, jObject);
					tree.remove(i);
				}
				
				System.out.println("Size of B+Tree after Deletion "+tree.size());
			
			// Commiting in database
			recman.commit();
						
		// SAving in the file
				table_file.delete() ;
				table_file.createNewFile() ;
				FileOutputStream fos = new FileOutputStream(file_name);
				fos.write(jArray.toJSONString().getBytes());
				fos.flush();
				fos.close();
			
		
		} catch (IOException | ParseException e) {
			// TODO Auto-generated catch block
			System.out.println("Error loading the file /n Debugging inside loading the table");
			
			e.printStackTrace();
		}
		
	}
	
	//------------------------------------------------------------------------------------------------------------------------------------------
	// Query with two arguments

	else if(qs_array.length == 11 ){
	//	System.out.println("Inside two argument ");
		
		
		String table_name = qs_array[2];
		
		String col1 = qs_array[4];
		String val1 = qs_array[6];
		String condition_operator1 = qs_array[5];
		
		String col2 =  qs_array[8];
		String val2 = qs_array[10];
		String condition_operator2 = qs_array[9];
		
		String logical_operator = qs_array[7];
		
		String file_name = table_name.concat(".json");
		String BTree_name = table_name.concat("_bt_pid");
		
		
		// Loading the table and the BTree
		File table_file = new File(file_name);
		long recid = -1 ;
		try {
			 recid = recman.getNamedObject(BTree_name);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Error in reading BTree inside delete Query");
			e.printStackTrace();
		}
		// Check2 if table is doesn't exit 
		if(!table_file.exists()){
			System.out.println("Table dosen't exist \n Debugging info inside delete function");
			return ;
		}
		// Check3, if table is empty
		if(recid == -1){
			System.out.println("Table is empty. \n Debugging info inside the delete function");
			return ;
		}
		
		// Iterating through json and Btree
		BTree tree = null;
		try {
			tree = BTree.load(recman, recid);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Error Loading the tree inside the delete function check 4");
			e.printStackTrace();
		}
		if(tree.size() == 0){
			System.out.println("Table is Empty.");
			return ;
		}
		// *****HACK******
		if(col1.equals("pid_".concat(table_name))||col2.equals("pid_".concat(table_name))){
			System.out.println("Performing the Deletion operation using B+Tree");
			//System.out.println("Intial Size of BTree is "+tree.size());
				
		}
		//----------------------
		
		// Loading the table
		JSONParser parser = new JSONParser();
		JSONArray jArray ;
		JSONObject jObject ;
		ArrayList<Integer> temp1 = new ArrayList<Integer>();
		ArrayList<Integer> temp2 = new ArrayList<Integer>();
		try {
			Object ob = parser.parse(new FileReader(file_name));
			jArray = (JSONArray)ob ;
//***************************************************************************************************************************************************			
			//  4. and , = , = 
			
				if(logical_operator.equals("AND") && condition_operator1.equals("=") && condition_operator2.equals("=")){
					for(int i = 0 ; i<jArray.size() ; i++){
						jObject = (JSONObject)jArray.get(i);
						Set keyset = jObject.keySet() ;
						for(Object o : keyset){
							if(o.toString().toLowerCase().equals(col1)){
								if(jObject.get(col1).equals(val1)){
									temp1.add(i);
								}
							}
						}
					}
					// iterating through the results of first operation
					for(int i : temp1 ){
						jObject = (JSONObject)jArray.get(i);
						Set keyset = jObject.keySet() ;
						for(Object o : keyset){
							if(o.toString().toLowerCase().equals(col2)){
								try{
								
								if(jObject.get(col2).equals(val2)){
								temp2.add(i);	
								}
							}
								catch(NumberFormatException e){
									System.out.println("Invalid attrbute to perform the query. \n Debugging inside the delete condition < ");
									e.printStackTrace();
								}
							}
						}
					}
					
				}
		
//***************************************************************************************************************************************************		
			// 5. and, = , < 
			else if(logical_operator.equals("AND") && condition_operator1.equals("=") && condition_operator2.equals("<")){
				for(int i = 0 ; i<jArray.size() ; i++){
					jObject = (JSONObject)jArray.get(i);
					Set keyset = jObject.keySet() ;
					for(Object o : keyset){
						if(o.toString().toLowerCase().equals(col1)){
							if(jObject.get(col1).equals(val1)){
								temp1.add(i);
							}
						}
					}
				}
				// iterating through results found via first condition
				for(int i : temp1){
					jObject = (JSONObject)jArray.get(i);
					Set keyset = jObject.keySet() ;
					for(Object o : keyset){
						if(o.toString().toLowerCase().equals(col2)){
							try{
							Integer col2_val = Integer.parseInt((String)jObject.get(col2)) ;
							Integer input_val2 = Integer.parseInt(val2) ;
							
							if(Integer.compare(col2_val,input_val2) < 0){
							temp2.add(i);	
							}
						}
							catch(NumberFormatException e){
								System.out.println("Invalid attrbute to perform the query. \n Debugging inside the delete condition < ");
								e.printStackTrace();
							}
						}
					}
				}
				
			}
			
//***********************************************************************************************************************************************************
			// 6. and = >
			else if(logical_operator.equals("AND") && condition_operator1.equals("=") && condition_operator2.equals(">")){
				for(int i = 0 ; i<jArray.size() ; i++){
					jObject = (JSONObject)jArray.get(i);
					Set keyset = jObject.keySet() ;
					for(Object o : keyset){
						if(o.toString().toLowerCase().equals(col1)){
							if(jObject.get(col1).equals(val1)){
								temp1.add(i);
							}
						}
					}
				}
				// iterating through results found via first condition
				for(int i : temp1){
					jObject = (JSONObject)jArray.get(i);
					Set keyset = jObject.keySet() ;
					for(Object o : keyset){
						if(o.toString().toLowerCase().equals(col2)){
							try{
							Integer col2_val = Integer.parseInt((String)jObject.get(col2)) ;
							Integer input_val2 = Integer.parseInt(val2) ;
							
							if(Integer.compare(col2_val,input_val2) > 0){
							temp2.add(i);	
							}
						}
							catch(NumberFormatException e){
								System.out.println("Invalid attrbute to perform the query. \n Debugging inside the delete condition = , >");
								e.printStackTrace();
							}
						}
					}
				}
				
			}
			
//***********************************************************************************************************************************************************			
//	7. AND > =
			else if(logical_operator.equals("AND") && condition_operator1.equals(">") && condition_operator2.equals("=")){
				for(int i = 0 ; i<jArray.size() ; i++){
					jObject = (JSONObject)jArray.get(i);
					Set keyset = jObject.keySet() ;
					for(Object o : keyset){
						if(o.toString().toLowerCase().equals(col2)){
							if(jObject.get(col2).equals(val2)){
								temp1.add(i);
							}
						}
					}
				}
				// iterating through results found via first condition
				for(int i : temp1){
					jObject = (JSONObject)jArray.get(i);
					Set keyset = jObject.keySet() ;
					for(Object o : keyset){
						if(o.toString().toLowerCase().equals(col1)){
							try{
							Integer col1_val = Integer.parseInt((String)jObject.get(col1)) ;
							Integer input_val1 = Integer.parseInt(val1) ;
							
							if(Integer.compare(col1_val,input_val1) > 0){
							temp2.add(i);	
							}
						}
							catch(NumberFormatException e){
								System.out.println("Invalid attrbute to perform the query. \n Debugging inside the delete condition = , >");
								e.printStackTrace();
							}
						}
					}
				}
				
			}
			
			
//***************************************************************************************************************************************************8
			//8. AND > >
			else if(logical_operator.equals("AND") && condition_operator1.equals(">") && condition_operator2.equals(">")){

				for(int i = 0 ; i<jArray.size() ; i++){
									jObject = (JSONObject)jArray.get(i);
									Set keyset = jObject.keySet() ;
									for(Object o : keyset){
										if(o.toString().toLowerCase().equals(col1)){
											try{
											Integer col1_val = Integer.parseInt((String)jObject.get(col1)) ;
											Integer input_val1 = Integer.parseInt(val1) ;
											
											if(Integer.compare(col1_val,input_val1) > 0){
											temp1.add(i);	
											}
										}
											catch(NumberFormatException e){
												System.out.println("Invalid attrbute to perform the query. \n Debugging inside the delete condition = , >");
												e.printStackTrace();
											}
										}
									}

					}

					// iterating through results found via first condition
								for(int i : temp1){
									jObject = (JSONObject)jArray.get(i);
									Set keyset = jObject.keySet() ;
									for(Object o : keyset){
										if(o.toString().toLowerCase().equals(col2)){
											try{
											Integer col2_val = Integer.parseInt((String)jObject.get(col2)) ;
											Integer input_val2 = Integer.parseInt(val2) ;
											
											if(Integer.compare(col2_val,input_val2) > 0){
											temp2.add(i);	
											}
										}
											catch(NumberFormatException e){
												System.out.println("Invalid attrbute to perform the query. \n Debugging inside the delete condition > , >");
												e.printStackTrace();
											}
										}
									}
								}




				}
//*************************************************************************************************************************************************************************			
//		9 . AND > <
			else if(logical_operator.equals("AND") && condition_operator1.equals(">") && condition_operator2.equals(">")){

				for(int i = 0 ; i<jArray.size() ; i++){
									jObject = (JSONObject)jArray.get(i);
									Set keyset = jObject.keySet() ;
									for(Object o : keyset){
										if(o.toString().toLowerCase().equals(col1)){
											try{
											Integer col1_val = Integer.parseInt((String)jObject.get(col1)) ;
											Integer input_val1 = Integer.parseInt(val1) ;
											
											if(Integer.compare(col1_val,input_val1) > 0){
											temp1.add(i);	
											}
										}
											catch(NumberFormatException e){
												System.out.println("Invalid attrbute to perform the query. \n Debugging inside the delete condition = , >");
												e.printStackTrace();
											}
										}
									}

					}

					// iterating through results found via first condition
								for(int i : temp1){
									jObject = (JSONObject)jArray.get(i);
									Set keyset = jObject.keySet() ;
									for(Object o : keyset){
										if(o.toString().toLowerCase().equals(col2)){
											try{
											Integer col2_val = Integer.parseInt((String)jObject.get(col2)) ;
											Integer input_val2 = Integer.parseInt(val2) ;
											
											if(Integer.compare(col2_val,input_val2) < 0){
											temp2.add(i);	
											}
										}
											catch(NumberFormatException e){
												System.out.println("Invalid attrbute to perform the query. \n Debugging inside the delete condition > , <");
												e.printStackTrace();
											}
										}
									}
								}




				}
			
//*****************************************************************************************************************************************************************
		//	10 . AND < = 
			
			else if(logical_operator.equals("AND") && condition_operator1.equals("<") && condition_operator2.equals("=")){
				for(int i = 0 ; i<jArray.size() ; i++){
					jObject = (JSONObject)jArray.get(i);
					Set keyset = jObject.keySet() ;
					for(Object o : keyset){
						if(o.toString().toLowerCase().equals(col1)){
							if(jObject.get(col2).equals(val2)){
								temp1.add(i);
							}
						}
					}
				}
				// iterating through results found via first condition
				for(int i : temp1){
					jObject = (JSONObject)jArray.get(i);
					Set keyset = jObject.keySet() ;
					for(Object o : keyset){
						if(o.toString().toLowerCase().equals(col1)){
							try{
							Integer col1_val = Integer.parseInt((String)jObject.get(col1)) ;
							Integer input_val1 = Integer.parseInt(val1) ;
							
							if(Integer.compare(col1_val,input_val1) < 0){
							temp2.add(i);	
							}
						}
							catch(NumberFormatException e){
								System.out.println("Invalid attrbute to perform the query. \n Debugging inside the delete condition < ");
								e.printStackTrace();
							}
						}
					}
				}
				
			}
//******************************************************************************************************************************************************			
			// 11. AND < <
			
			else if(logical_operator.equals("AND") && condition_operator1.equals("<") && condition_operator2.equals("<")){

				for(int i = 0 ; i<jArray.size() ; i++){
									jObject = (JSONObject)jArray.get(i);
									Set keyset = jObject.keySet() ;
									for(Object o : keyset){
										if(o.toString().toLowerCase().equals(col1)){
											try{
											Integer col1_val = Integer.parseInt((String)jObject.get(col1)) ;
											Integer input_val1 = Integer.parseInt(val1) ;
											
											if(Integer.compare(col1_val,input_val1) < 0){
											temp1.add(i);	
											}
										}
											catch(NumberFormatException e){
												System.out.println("Invalid attrbute to perform the query. \n Debugging inside the delete condition = , >");
												e.printStackTrace();
											}
										}
									}

					}

					// iterating through results found via first condition
								for(int i : temp1){
									jObject = (JSONObject)jArray.get(i);
									Set keyset = jObject.keySet() ;
									for(Object o : keyset){
										if(o.toString().toLowerCase().equals(col2)){
											try{
											Integer col2_val = Integer.parseInt((String)jObject.get(col2)) ;
											Integer input_val2 = Integer.parseInt(val2) ;
											
											if(Integer.compare(col2_val,input_val2) < 0){
											temp2.add(i);	
											}
										}
											catch(NumberFormatException e){
												System.out.println("Invalid attrbute to perform the query. \n Debugging inside the delete condition > , <");
												e.printStackTrace();
											}
										}
									}
								}




				}
			
//*********************************************************************************************************************************************************************
			
			//12. AND < >
			
			else if(logical_operator.equals("AND") && condition_operator1.equals("<") && condition_operator2.equals(">")){

				for(int i = 0 ; i<jArray.size() ; i++){
									jObject = (JSONObject)jArray.get(i);
									Set keyset = jObject.keySet() ;
									for(Object o : keyset){
										if(o.toString().toLowerCase().equals(col1)){
											try{
											Integer col1_val = Integer.parseInt((String)jObject.get(col1)) ;
											Integer input_val1 = Integer.parseInt(val1) ;
											
											if(Integer.compare(col1_val,input_val1) < 0){
											temp1.add(i);	
											}
										}
											catch(NumberFormatException e){
												System.out.println("Invalid attrbute to perform the query. \n Debugging inside the delete condition = , >");
												e.printStackTrace();
											}
										}
									}

					}
						
					// iterating through results found via first condition
								for(int i : temp1){
									jObject = (JSONObject)jArray.get(i);
									Set keyset = jObject.keySet() ;
									for(Object o : keyset){
										if(o.toString().toLowerCase().equals(col2)){
											try{
											Integer col2_val = Integer.parseInt((String)jObject.get(col2)) ;
											Integer input_val2 = Integer.parseInt(val2) ;
											
											if(Integer.compare(col2_val,input_val2) > 0){
											temp2.add(i);	
											}
										}
											catch(NumberFormatException e){
												System.out.println("Invalid attrbute to perform the query.");
												e.printStackTrace();
											}
										}
									}
								}




				}
//****************************************************************************************************************************************************************
// 13 . OR = = 
			else if(logical_operator.equals("OR") && condition_operator1.equals("=") && condition_operator2.equals("=")){
				for(int i = 0 ; i<jArray.size() ; i++){
					jObject = (JSONObject)jArray.get(i);
					Set keyset = jObject.keySet() ;
					for(Object o : keyset){
						if(o.toString().toLowerCase().equals(col1)){
							if(jObject.get(col1).equals(val1)){
								temp2.add(i);
							}
						}
					}
					for(Object o : keyset){
						if(o.toString().toLowerCase().equals(col2)){
							if(jObject.get(col2).equals(val2)){
								if(!temp2.contains(i))
								{
									temp2.add(i) ;
								}

							}
						}
					}
				}
}
			
//*********************************************************************************************************************************************
				// 14 OR = >
				
			else if(logical_operator.equals("OR") && condition_operator1.equals("=") && condition_operator2.equals(">")){
				for(int i = 0 ; i<jArray.size() ; i++){
					jObject = (JSONObject)jArray.get(i);
					Set keyset = jObject.keySet() ;
					for(Object o : keyset){
						if(o.toString().toLowerCase().equals(col1)){
							if(jObject.get(col1).equals(val1)){
								temp2.add(i);
							}
						}
					}
					for(Object o : keyset){
						if(o.toString().toLowerCase().equals(col2)){
							try{
							Integer col2_val = Integer.parseInt((String)jObject.get(col2)) ;
							Integer input_val2 = Integer.parseInt(val2) ;
							
							if(Integer.compare(col2_val,input_val2) > 0){
								if(!temp2.contains(i))
								temp2.add(i);	
							}
						}
						catch(NumberFormatException e){
								System.out.println("Invalid attrbute to perform the query. \n Debugging inside the OR ");
								e.printStackTrace();
							}

			     	}
				}
			}
		}
//**********************************************************************************************************************************************
						//15 OR = <
						else if(logical_operator.equals("OR") && condition_operator1.equals("=") && condition_operator2.equals("<")){
							for(int i = 0 ; i<jArray.size() ; i++){
								jObject = (JSONObject)jArray.get(i);
								Set keyset = jObject.keySet() ;
								for(Object o : keyset){
									if(o.toString().toLowerCase().equals(col1)){
										if(jObject.get(col1).equals(val1)){
											temp2.add(i);
										}
									}
								}
								for(Object o : keyset){
									if(o.toString().toLowerCase().equals(col2)){
										try{
										Integer col2_val = Integer.parseInt((String)jObject.get(col2)) ;
										Integer input_val2 = Integer.parseInt(val2) ;
										
										if(Integer.compare(col2_val,input_val2) < 0){
											if(!temp2.contains(i))
											temp2.add(i);	
										}
									}
									catch(NumberFormatException e){
											System.out.println("Invalid attrbute to perform the query. \n Debugging inside the delete condition or = <");
											e.printStackTrace();
										}

							}
						}
					}
				}
//**********************************************************************************************************************************************************
				//16 OR < <
				
						else if(logical_operator.equals("OR") && condition_operator1.equals("<") && condition_operator2.equals("<")){
							for(int i = 0 ; i<jArray.size() ; i++){
								jObject = (JSONObject)jArray.get(i);
								Set keyset = jObject.keySet() ;
								for(Object o : keyset){
									if(o.toString().toLowerCase().equals(col1)){
										try{
										Integer col1_val = Integer.parseInt((String)jObject.get(col1)) ;
										Integer input_val1 = Integer.parseInt(val1) ;
										
										if(Integer.compare(col1_val,input_val1) < 0){
										
											temp2.add(i);	
										}
									}
										catch(NumberFormatException e){
											System.out.println("Invalid attrbute to perform the query. \n Debugging inside the delete condition ");
											e.printStackTrace();
										}
									}
								}
								for(Object o : keyset){
									if(o.toString().toLowerCase().equals(col2)){
										try{
										Integer col2_val = Integer.parseInt((String)jObject.get(col2)) ;
										Integer input_val2 = Integer.parseInt(val2) ;
										
										if(Integer.compare(col2_val,input_val2) < 0){
											if(!temp2.contains(i))
											temp2.add(i);	
										}
									}
									catch(NumberFormatException e){
											System.out.println("Invalid attrbute to perform the query. \n Debugging inside the delete condition ");
											e.printStackTrace();
										}

							}
						}
					}
				}
//***************************************************************************************************************************************************************************								
//17 OR < >
						else if(logical_operator.equals("OR") && condition_operator1.equals("<") && condition_operator2.equals(">")){
							for(int i = 0 ; i<jArray.size() ; i++){
								jObject = (JSONObject)jArray.get(i);
								Set keyset = jObject.keySet() ;
								for(Object o : keyset){
									if(o.toString().toLowerCase().equals(col1)){
										try{
										Integer col1_val = Integer.parseInt((String)jObject.get(col1)) ;
										Integer input_val1 = Integer.parseInt(val1) ;
										
										if(Integer.compare(col1_val,input_val1) < 0){
										
											temp2.add(i);	
										}
									}
										catch(NumberFormatException e){
											System.out.println("Invalid attrbute to perform the query. \n Debugging inside the delete condition ");
											e.printStackTrace();
										}
									}
								}
								for(Object o : keyset){
									if(o.toString().toLowerCase().equals(col2)){
										try{
										Integer col2_val = Integer.parseInt((String)jObject.get(col2)) ;
										Integer input_val2 = Integer.parseInt(val2) ;
										
										if(Integer.compare(col2_val,input_val2) > 0){
											if(!temp2.contains(i))
											temp2.add(i);	
										}
									}
									catch(NumberFormatException e){
											System.out.println("Invalid attrbute to perform the query. \n Debugging inside the delete condition ");
											e.printStackTrace();
										}

							}
						}
					}
				}
//*******************************************************************************************************************************************************
// 18 OR < =
				
						else if(logical_operator.equals("OR") && condition_operator1.equals("<") && condition_operator2.equals("=")){
							for(int i = 0 ; i<jArray.size() ; i++){
								jObject = (JSONObject)jArray.get(i);
								Set keyset = jObject.keySet() ;
								for(Object o : keyset){
									if(o.toString().toLowerCase().equals(col2)){
										if(jObject.get(col2).equals(val2)){
											temp2.add(i);
										}
									}
								}
								for(Object o : keyset){
									if(o.toString().toLowerCase().equals(col1)){
										try{
										Integer col1_val = Integer.parseInt((String)jObject.get(col1)) ;
										Integer input_val1 = Integer.parseInt(val1) ;
										
										if(Integer.compare(col1_val,input_val1) < 0){
											if(!temp1.contains(i))
											temp2.add(i);	
										}
									}
									catch(NumberFormatException e){
											System.out.println("Invalid attrbute to perform the query. \n Debugging inside the OR ");
											e.printStackTrace();
										}

						     	}
							}
						}
					}
//*****************************************************************************************************************************************************************
				//19. OR > =
						else if(logical_operator.equals("OR") && condition_operator1.equals(">") && condition_operator2.equals("=")){
							for(int i = 0 ; i<jArray.size() ; i++){
								jObject = (JSONObject)jArray.get(i);
								Set keyset = jObject.keySet() ;
								for(Object o : keyset){
									if(o.toString().toLowerCase().equals(col2)){
										if(jObject.get(col2).equals(val2)){
											temp2.add(i);
										}
									}
								}
								for(Object o : keyset){
									if(o.toString().toLowerCase().equals(col1)){
										try{
										Integer col1_val = Integer.parseInt((String)jObject.get(col1)) ;
										Integer input_val1 = Integer.parseInt(val1) ;
										
										if(Integer.compare(col1_val,input_val1) > 0){
											if(!temp1.contains(i))
											temp2.add(i);	
										}
									}
									catch(NumberFormatException e){
											System.out.println("Invalid attrbute to perform the query. \n Debugging inside the OR ");
											e.printStackTrace();
										}

						     	}
							}
						}
					}
//*********************************************************************************************************************************************************************
				//20 OR > >
						else if(logical_operator.equals("OR") && condition_operator1.equals(">") && condition_operator2.equals(">")){
							for(int i = 0 ; i<jArray.size() ; i++){
								jObject = (JSONObject)jArray.get(i);
								Set keyset = jObject.keySet() ;
								for(Object o : keyset){
									if(o.toString().toLowerCase().equals(col1)){
										try{
										Integer col1_val = Integer.parseInt((String)jObject.get(col1)) ;
										Integer input_val1 = Integer.parseInt(val1) ;
										
										if(Integer.compare(col1_val,input_val1) > 0){
										
											temp2.add(i);	
										}
									}
										catch(NumberFormatException e){
											System.out.println("Invalid attrbute to perform the query. \n Debugging inside the delete condition ");
											e.printStackTrace();
										}
									}
								}
								for(Object o : keyset){
									if(o.toString().toLowerCase().equals(col2)){
										try{
										Integer col2_val = Integer.parseInt((String)jObject.get(col2)) ;
										Integer input_val2 = Integer.parseInt(val2) ;
										
										if(Integer.compare(col2_val,input_val2) > 0){
											if(!temp2.contains(i))
											temp2.add(i);	
										}
									}
									catch(NumberFormatException e){
											System.out.println("Invalid attrbute to perform the query. \n Debugging inside the delete condition ");
											e.printStackTrace();
										}

							}
						}
					}
				}
//**********************************************************************************************************************************************
				//21 OR > <
						else if(logical_operator.equals("OR") && condition_operator1.equals(">") && condition_operator2.equals("<")){
							for(int i = 0 ; i<jArray.size() ; i++){
								jObject = (JSONObject)jArray.get(i);
								Set keyset = jObject.keySet() ;
								for(Object o : keyset){
									if(o.toString().toLowerCase().equals(col1)){
										try{
										Integer col1_val = Integer.parseInt((String)jObject.get(col1)) ;
										Integer input_val1 = Integer.parseInt(val1) ;
										
										if(Integer.compare(col1_val,input_val1) > 0){
										
											temp2.add(i);	
										}
									}
										catch(NumberFormatException e){
											System.out.println("Invalid attrbute to perform the query. \n Debugging inside the delete condition ");
											e.printStackTrace();
										}
									}
								}
								for(Object o : keyset){
									if(o.toString().toLowerCase().equals(col2)){
										try{
										Integer col2_val = Integer.parseInt((String)jObject.get(col2)) ;
										Integer input_val2 = Integer.parseInt(val2) ;
										
										if(Integer.compare(col2_val,input_val2) < 0){
											if(!temp2.contains(i))
											temp2.add(i);	
										}
									}
									catch(NumberFormatException e){
											System.out.println("Invalid attrbute to perform the query. \n Debugging inside the delete condition ");
											e.printStackTrace();
										}

							}
						}
					}
				}
// End of all cases
//-------------------------------------------------------------------------------------------------------------------------------------------
			// Updating database and json after update
			
			System.out.println("Intial Size of B+Tree "+tree.size());
			for(int i : temp2){
				jArray.remove(i);
				jObject = new JSONObject();
				jArray.add(i, jObject);
				tree.remove(i);
			}
			
			System.out.println("Size of B+Tree after Deletion "+tree.size());
		
		// Commiting in database
		recman.commit();
					
	// SAving in the file
			table_file.delete() ;
			table_file.createNewFile() ;
			FileOutputStream fos = new FileOutputStream(file_name);
			fos.write(jArray.toJSONString().getBytes());
			fos.flush();
			fos.close();
			
			
	
		}catch(Exception e){
			System.out.println("Invalid Query");
			
		}
			
		}
	

	
	 
	
	
	
}




private static void updateQuery(String query) {
	 
	query = query.replace("UPDATE", "update"); 
	query = query.replace("SET", "set");
	query = query.replace("WHERE", "where"); 
	query = query.replace("AND", "and"); 
	query = query.replace("OR", "or"); 
	query = query.replace("\'", ""); 
	String tableName = "";
	String setString = "";
	String whereString = "";
	boolean andClause = false ; 
	boolean orClause = false ; 
	boolean setAll = false ; 
	boolean invalid = false ; 
	ArrayList<String> whereClauses = new ArrayList<String>(); 
	
	System.out.println("Update query after replace - " + query );
	
   
	
	if(query.startsWith("update")&&query.contains("set")){
	 
	tableName = query.split("update")[1].split("set")[0].trim();
	if(tables.get(tableName)==null)
	invalid = true ; 
	if(query.contains("where")){
	whereString = query.split("where")[1].trim();
	setString = query.split("set")[1].split("where")[0].trim();
	
	}
	else setString = query.split("set")[1].trim();
	
	if(!whereString.equals("")){
	
	if(whereString.contains("and")) {
	andClause = true ; 
	}
	else if(whereString.contains("or")){
	orClause = true ; 
	}
	
	if(andClause || orClause){
	String wheres[] ; 
	if(andClause){
	wheres = whereString.split("and"); 
	for(String c: wheres){
	whereClauses.add(c.trim());
	}
	}
	else if(orClause){
	wheres = whereString.split("or"); 
	for(String c: wheres){
	whereClauses.add(c.trim());
	}
	}
	}
	
	else whereClauses.add(whereString.trim());
	
	}
	
	else if(whereString.equals("")&&!setString.equals("")){
	setAll = true ; 
	}
	
	ArrayList<String> setColumns = new ArrayList() ; 
	
	if(setString.contains(",")){
	String cols[] = setString.split(","); 
	for(String c:cols){
	setColumns.add(c.trim());
	}
	}
	
	else if(!setString.trim().equals("")){
	setColumns.add(setString.trim());
	}
	
	else invalid = true ; 
	
	if(setString.contains("pid_")) invalid = true ; 
	/*System.out.println("Set - " + setColumns);
	System.out.println("Table Name - " + tableName);
	for(String s : whereClauses)
	System.out.println("Where - " + s);
	System.out.println("Invalid - " + invalid );*/
	HashMap<String,String> schema = getSchemaMap(tableName);
	
	if(!invalid){
	ArrayList<Integer> indices = new ArrayList<Integer>(); 
	String fName = tableName.trim() + ".json";
	File f = new File(fName);
	JSONArray jsonArray = new JSONArray(); 
	JSONParser parser = new JSONParser();
	System.out.println("File Name  - " + fName);
	 
	if(f.exists()&&f.length()!=0){
	Object ob;
	try {
	ob = parser.parse(new FileReader(fName));
	jsonArray = (JSONArray)ob;
	//System.out.println("Size " + jsonArray.size());
	} catch (IOException | ParseException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
	}
	
	if(setAll||whereString.trim().equals("")){
	
	JSONObject jOld = new JSONObject() ; 
	HashMap<String, String> newVals = new HashMap<String,String>(); 
	String k , v ; 
	for(String c:setColumns){
	k = c.split("=")[0].trim(); 
	v = c.split("=")[1].trim(); 
	newVals.put(k, v); 
	}
	 
	int s = jsonArray.size();
	
	 for(int i=0;i<=s-1;i++){
	jOld = (JSONObject) jsonArray.get(i);
	for (Map.Entry<String, String> entry : newVals.entrySet()) {
	 	//schemaBig.put(entry.getKey(), entry.getValue());
	 	jOld.put(entry.getKey(), entry.getValue());
	 	}
	 	
	 	jsonArray.remove(i); 
	 	jsonArray.add(i,jOld);
	}
	}
	
	else {
	
	
	if(whereString.contains("pid_")){
	//System.out.println("Entered PID case ...");
	boolean range = false ; 
	int first = 0 , last = 0 ; 
	String pid_clause = whereClauses.get(0); 
	String operator = ""; 
	if(pid_clause.contains("=")) operator = "="; 
	else if(pid_clause.contains(">")){
	range = true ; 
	operator = ">"; 
	}
	else if(pid_clause.contains("<")){
	range = true ; 
	operator = "<"; 
	}
	
	String pid_value = pid_clause.split(operator)[1].trim();
	int v =  Integer.valueOf(pid_value); 
	if(range){
	if(operator.equals(">")){
	
	first = v+1; 
	last = jsonArray.size()-1 ; 
	}
	
	if(operator.equals("<")){
	
	first = 0 ; 
	last = v-1;
	}
	
	}
	String otherClause = "";
	String otherOperator = ""; 
	String otherValue = ""; 
	String otherCol = ""; 
	boolean other = false ; 
	 
	if(whereClauses.size()>1){
	other = true ; 
	otherClause = whereClauses.get(1);
	if(otherClause.contains("=")) otherOperator = "="; 
	else if(otherClause.contains(">")) otherOperator = ">"; 
	else if(otherClause.contains("<")) otherOperator = "<"; 
	otherValue = otherClause.split(otherOperator)[1].trim();
	otherCol = otherClause.split(otherOperator)[0].trim();
	}
	
	   JSONObject jNew = new JSONObject(); 
	   JSONObject jOld ; 
	/*   
	   System.out.println("===========================");
	   System.out.println("PID value  - "+ pid_value);
	   System.out.println("Operator - "+ operator);
	   System.out.println("Range - " + first + " " + last);
	   System.out.println("Other col --" + otherCol);
	   System.out.println("Other value --" + otherValue);
	   System.out.println("Other Operator - "+ otherOperator);
	   System.out.println("Array Index = " + v);
	   System.out.println("===========================");*/
	   if(operator.equals("=")){
	   String k , newVal ; 
	   jOld = (JSONObject)jsonArray.get(v); 
	   //System.out.println("Json Object - " + jOld);
	   for(String c:setColumns){
	   k = c.split("=")[0].trim(); 
	   newVal = c.split("=")[1].trim();
	   //System.out.println("Key - Value = " + k + " , " + newVal);
	   if(jOld.containsKey(k)){
	   //System.out.println("Matched pid key .. ");
	   if(other){
	   //System.out.println("Old k,v " + otherCol + " , " + jOld.get(otherCol) );
	   if(jOld.get(otherCol).equals(otherValue)){
	   jOld.put(k, newVal);
	   //System.out.println("Updated ..");
	   
	   }
	   
	   }
	   else jOld.put(k, newVal); 
	   }
	   }
	    
	   
	   jsonArray.remove(v); 
	   jsonArray.add(v, jOld);
	   
	    
	   
	   }
	   
	   else if(range){
	   String k , newVal ; 
	   for(int i = first; i<=last ; i++){
	   jOld = (JSONObject)jsonArray.get(i); 
	    
	   for(String c:setColumns){
	   k = c.split("=")[0].trim(); 
	   newVal = c.split("=")[1].trim();
	   if(jOld.containsKey(k)){
	   if(other){
	   if(jOld.get(otherCol).toString().equals(otherValue))
	   jOld.put(k, newVal);
	   }
	   else jOld.put(k, newVal); 
	   }
	   }
	   
	   jsonArray.remove(i); 
	   jsonArray.add(i, jOld);
	   } //end for 
	   }
	   
	  
	 
	 
	}  // end pid case 
	
	
	 else {
	
	//search file and update 
	//System.out.println("Non pid - search entire file");
	String firstWhere = whereClauses.get(0); 
	String secondWhere =""; 
	boolean second = false ; 
	if(whereClauses.size()>1){ 
	//System.out.println("Second set to true");
	second = true ; 
	secondWhere = whereClauses.get(1);
	}
	
	String firstCol ="" , firstOp ="" , firstVal ="" ; 
	String secondCol = "" , secondOp = "" , secondVal = "" ; 
	
	if(firstWhere.contains("=")) firstOp = "="; 
	else if(firstWhere.contains(">")) firstOp = ">"; 
	else if(firstWhere.contains("<")) firstOp = "<"; 
	
	firstCol = firstWhere.split(firstOp)[0].trim(); 
	firstVal = firstWhere.split(firstOp)[1].trim(); 
	
	if(second){
	//System.out.println("Inside second");
	if(secondWhere.contains("=")) secondOp = "="; 
	else if(secondWhere.contains(">")) secondOp = ">"; 
	else if(secondWhere.contains("<")) secondOp = "<"; 
	
	secondCol = secondWhere.split(secondOp)[0].trim(); 
	secondVal = secondWhere.split(secondOp)[1].trim();
	 
	}
	JSONObject jOld = new JSONObject() ; 
	int size = jsonArray.size() ; 
	HashMap<String, String> newVals = new HashMap<String,String>(); 
	String k , v ; 
	for(String c:setColumns){
	k = c.split("=")[0].trim(); 
	v = c.split("=")[1].trim(); 
	newVals.put(k, v); 
	}
	
	 
	if(schema.get(firstCol)==null){
	//System.out.println("Col -  " + firstCol + " not found in schema.");
	invalid = true ; 
	}
	
	
	if(!secondCol.equals("")){
	if(schema.get(secondCol)==null){
	
	invalid = true ; 
	//System.out.println("Col -  " + secondCol + " not found in schema.");
	}
	
	}
	
	boolean match = false ;
	boolean halfMatch = false ; 
	String v1 ="", v2="" ; 
	
	/*System.out.println("First " + firstOp + " | " + firstVal + " | "+ firstCol);
	System.out.println("Second op " + secondOp);
	System.out.println("Second col " + secondCol);
	System.out.println("Second val " + secondVal);
	System.out.println("Second operator - " + second );
	*/
	int v1_int = 0, v1_compare = 0 ;
	int v2_int = 0, v2_compare = 0 ;
	if(!invalid){
	for(int i=0; i<=size-1; i++){
	try {
	match = false ; 
	halfMatch = false ; 
	 	if(jsonArray.get(i)!=null)
	 	jOld = (JSONObject)jsonArray.get(i);
	if(jOld.get(firstCol)!=null)
	v1 = (String)jOld.get(firstCol);
	 if(!firstOp.equals("=")){
	  if(v1!=null){
	  v1_int = Integer.valueOf(v1);
	  v1_compare = Integer.valueOf(firstVal); 
	  }
	  
	 if(firstOp.equals(">")){
	 if(v1_int > v1_compare){
	 halfMatch = true ; 
	 }
	 }
	 
	 else if(firstOp.equals("<")){
	 if(v1_int<v1_compare)
	 halfMatch = true ; 
	 }
	 
	 }
	 else if(firstOp.equals("=")){
	 if(v1.equals(firstVal))
	 halfMatch = true ; 
	 }
	 
	 //second where 
	 if(!second&&halfMatch){
	 match = true ; 
	 }
	 
	 else if(second&&halfMatch){
	 if(jOld.get(jOld.get(secondCol))!=null)
	 v2 = jOld.get(secondCol).toString();
	 if(!secondOp.equals("=")){
	 if(v2!=null){
	 v2_int = Integer.valueOf(v2);
	 v2_compare = Integer.valueOf(secondVal); 
	 }
	 
	 if(secondOp.equals(">")){
	 if(v2_int > v2_compare){
	 match = true ; 
	 }
	 }
	 
	 else if(secondOp.equals("<")){
	 if(v2_int<v2_compare)
	 match = true ; 
	 }
	 
	 }
	 else if(secondOp.equals("=")){
	 if(v2.equals(secondVal))
	 match = true ; 
	 }
	 }
	 
	
	 	 if(match){
	 	for (Map.Entry<String, String> entry : newVals.entrySet()) {
	 	//schemaBig.put(entry.getKey(), entry.getValue());
	 	jOld.put(entry.getKey(), entry.getValue());
	 	}
	 	
	 	jsonArray.remove(i); 
	 	jsonArray.add(i,jOld);
	 	
	 	
	 	 }
	}
	
	
	
	catch (Exception e) {
	//System.out.println("Exception ..");
	}
	
	 
	 
	 
	}// end for - iteration on entire loop 
	 
	
	
	
	}
	
	 	}
	
	}
	
	
	
	try {
	f.delete();
	f.createNewFile();
	System.out.println("Writing to file ..");
	//FileOutputStream fos = new FileOutputStream(fName);
	//fos.close();
	FileOutputStream writer = new FileOutputStream(fName);
	writer.write(jsonArray.toJSONString().getBytes());
	writer.flush();
	writer.close();
	
	} catch (IOException e) {
	// TODO Auto-generated catch block
	System.out.println("Error updating in json file.");
	}	
	
	} //end innner invalid check 
	
	}
	}
	
	}//end first basic check of query 

private static void selectQuery(String query) {
	//System.out.println("Inside select menthod - " + query);
	boolean invalidQuery = false ; 
	String whereString = ""; 
	String fromString = ""; 
	String selectString = ""; 
	String orderByString = "";
	boolean star = false ; 
	boolean andClause = false ; 
	boolean orClause = false  ;
	boolean join = false ; 
	ArrayList<String> whereClauses = new ArrayList<String>(); 
	
	 
	query = query.replace("Select", "select"); 
	 
	query.replace("SELECT", "select"); 
	
	
	query = query.replace("FROM", "from"); 
	query = query.replace("From", "from"); 
	
	query = query.replace("WHERE", "where"); 
	query = query.replace("Where", "where"); 
	
	query = query.replace("ORDER BY", "order by");
	query = query.replace("Order By", "order by");
	
	query= query.replace("ASC", "asc");
	query= query.replace("Asc", "asc");
	
	query  = query.replace("DESC", "desc"); 
	query  = query.replace("Desc", "desc"); 
	
	query  = query.replace("AND", "and");
	query  = query.replace("And", "and");
	
	query  = query.replace("OR", "or"); 
	query  = query.replace("Or", "or"); 
	
	 
	//System.out.println("After replace " + query);
	
	if(
	(query.startsWith("select") )
	&&( query.contains("from"))
	 
	){
	try{
	 invalidQuery = false ; 
	 selectString = query.split("from")[0].split("select")[1].trim();
	 
	 if(selectString.trim().equals("")){
	 //System.out.println("select blank");
	 invalidQuery = true ; 
	 }
	
	 
	 else if(selectString.trim().equals("*"))
	 star = true ; 
	 
	 if(query.contains("where"))
	 fromString = query.split("where")[0].split("from")[1].trim();
	 
	 //else fromString = query.split("from")[1].trim();
	 
	 else if(!query.contains("where")&& !query.contains("order by"))
	 fromString = query.split("from")[1].trim();
	 else if(!query.contains("where")&& query.contains("order by"))
	 fromString = query.split("order by")[0].split("from")[1].trim();
	 
	 
	 if(fromString.equals("")){
	// System.out.println("From blank");
	 invalidQuery = true ; 
	 }
	
	 
	/* if(fromString.split(" ").length>1&&fromString.contains(",")){
	 System.out.println("Comma missing");
	 invalidQuery = true ; 
	 }*/
	 
	 
	 else if(fromString.contains(","))
	 join = true ; 
	 
	 if(query.contains("where")&&query.contains("order by"))
	 whereString = query.split("order by")[0].split("where")[1].trim();
	 
	 else if(query.contains("where"))
	 whereString = query.split("where")[1].trim();
	 
	 
	/*	 if(whereString.equals(""))
	 invalidQuery = true ; */
	 
	 if(whereString.contains("and")){
	 andClause = true ; 
	 String[] subQ = whereString.split("and");
	 for(int i=0; i<subQ.length;i++){
	 if(subQ[i].trim().contains("=")){
	 if(subQ[i].trim().split("=").length!=2)
	 invalidQuery = true ; 
	 else whereClauses.add(subQ[i].trim());
	 
	 }
	 else if(subQ[i].trim().contains("<")){
	 if(subQ[i].trim().split("<").length!=2)
	 invalidQuery = true ; 
	 else whereClauses.add(subQ[i].trim());
	 
	 }
	 
	else if(subQ[i].trim().contains(">")){
	 if(subQ[i].trim().split(">").length!=2)
	 invalidQuery = true ; 
	 else whereClauses.add(subQ[i].trim());
	}
	
	else invalidQuery = true  ; 
	 }
	 }
	 
	 
	 else if(whereString.contains("or")){
	 orClause = true ; 
	 String[] subQ = whereString.split("or");
	 for(int i=0; i<subQ.length;i++){
	 if(subQ[i].trim().contains("=")){
	 if(subQ[i].trim().split("=").length!=2)
	 invalidQuery = true ; 
	 else whereClauses.add(subQ[i].trim());
	 
	 }
	 else if(subQ[i].trim().contains("<")){
	 if(subQ[i].trim().split("<").length!=2)
	 invalidQuery = true ; 
	 else whereClauses.add(subQ[i].trim());
	 
	 }
	 
	else if(subQ[i].trim().contains(">")){
	 if(subQ[i].trim().split(">").length!=2)
	 invalidQuery = true ; 
	 else whereClauses.add(subQ[i].trim());
	}
	
	else invalidQuery = true  ; 
	
	 }
	 }
	 
	 else whereClauses.add(whereString);
	 
	 if(query.contains("order by")){
	 orderByString = query.split("order by")[1].trim();
	 if(orderByString.equals(""))
	 invalidQuery = true ; 
	 }
	 
	} //end try 
	
	 
	
	catch(Exception e){
	invalidQuery = true ; 
	}
	
	
	}//end main if 
	
	  
	else{
	System.out.println("Invalid Query - Missing Select or From !");
	
	}
	
	 
	
	if(invalidQuery){
	System.out.println("Invalid Query after checks ");
	}
	
	else{
	//start select -- 
	
	if(join){
	String clause = "and"; 
	if(orClause) clause = "or"; 
	
	selectJoin(selectString,fromString,whereClauses,orderByString, clause); 
	}
	else{
	
	String fName = fromString.trim() + ".json";
	File f = new File(fName);
	JSONArray jsonArray; 
	JSONParser parser = new JSONParser();
	//System.out.println("File Name  - " + fName);
	ArrayList<JSONArray> results = new ArrayList<JSONArray>();
	if(f.exists()&&f.length()!=0){
	Object ob;
	try {
	ob = parser.parse(new FileReader(fName));
	jsonArray = (JSONArray)ob;
	/*	System.out.println("Select Columns " + selectString);
	System.out.println("Table "  + fromString);
	System.out.println("Where String " + whereString);
	System.out.println("Order By " + orderByString);*/
	
	
	ArrayList<JSONArray> resultArrays = new ArrayList<JSONArray>(); 
	String col ; 
	
	
	for(String s:whereClauses){
	
	//System.out.println(s);
	if(s.contains("pid_")){
	//searchBTreePrim(fileName, indices)
	String bTreeFileName = fromString + "_bt_pid" ; 
	ArrayList<Integer> indices = searchBTree(bTreeFileName, s); 
	System.out.println("Returned from btree search - " + indices.size());
	JSONObject j ; 
	JSONArray indexResults = new JSONArray(); 
	for(int i:indices){
	
	if(jsonArray.get(i)!=null){
	j = (JSONObject) jsonArray.get(i); 
	if(!j.isEmpty()){
	indexResults.add(j);
	}
	}
	}
	if(!indexResults.isEmpty()){
	resultArrays.add(indexResults);
	//System.out.println("Total result arrays till now " + resultArrays.size());
	}
	
	}
	
	
	
	else{
	String categ = searchCategory(fromString,s);
	//System.out.println("Search from category - " + categ);
	if(categ.equals("")){
	JSONArray resultFromFile = searchFile(fromString, s, jsonArray); 
	//System.out.println("File results size - " + resultFromFile.size());
	if(resultFromFile != null && !resultFromFile.isEmpty()){
	resultArrays.add(resultFromFile); 
	//System.out.println("Total result arrays till now " + resultArrays.size());
	}
	}
	
	else{
	//System.out.println("Sending to btree");
	ArrayList<Integer> indicesFromTree = searchBTree(categ, s);
	JSONArray treeResults = new JSONArray(); 
	JSONObject jT; 
	for(int i:indicesFromTree){
	
	if(jsonArray.get(i)!=null){
	jT = (JSONObject) jsonArray.get(i); 
	if(!jT.isEmpty()){
	treeResults.add(jT);
	}
	}
	}
	if(!treeResults.isEmpty()){
	resultArrays.add(treeResults);
	}
	
	 
	}
	 
	}
	} // end where clauses 
	
	//System.out.println("TOTAL arrays to merge - " + resultArrays.size());
	JSONArray finalResults = new JSONArray(); 
	//start merging 
	if(!resultArrays.isEmpty()){
	//System.out.println("Inside Merge ");
	ArrayList<JSONArray> copy = new ArrayList<JSONArray>(); 
	JSONArray temp = new JSONArray(); 
	JSONArray temp1 = new JSONArray();
	
	if(resultArrays.size()==1){
	finalResults = resultArrays.get(0);
	}
	
	else if(resultArrays.size()==2){
	if(andClause)
	finalResults = intersection(resultArrays); 
	else if(orClause)
	finalResults = union(resultArrays);
	}
	
	else if(resultArrays.size()==3){
	  //System.out.println("Merge 3");
	   
	  copy.clear();
	  copy.add(resultArrays.get(0)); 
	  copy.add(resultArrays.get(1)); 
	  
	  //System.out.println("Copy size 1 - " + copy.size());
	  if(andClause)
	  temp = intersection(copy);
	  else if(orClause)
	  temp = union(copy);
	  
	  //System.out.println("Intersection 1 - " + temp.size());
	  copy.clear();
	  
	  copy.add(temp);
	  copy.add(resultArrays.get(2)); 
	  //System.out.println("Copy size 2- " + copy.size());
	  if(andClause)
	  finalResults = intersection(copy);
	  else if(orClause)
	  finalResults = union(copy);
	  
	 // System.out.println("Intersection 1 - " + finalResults.size());
	  
	}
	else if(resultArrays.size()==4){
	copy.clear();
	copy.add(resultArrays.get(0)); 
	copy.add(resultArrays.get(1)); 
	 if(andClause)
	  temp = intersection(copy);
	  else if(orClause)
	  temp = union(copy);
	 
	 copy.clear();
	 copy.add(temp);
	 copy.add(resultArrays.get(2));
	 
	 if(andClause)
	  temp = intersection(copy);
	  else if(orClause)
	  temp = union(copy);
	 
	 copy.clear();
	 copy.add(temp);
	 copy.add(resultArrays.get(3));
	 
	  if(andClause)
	  finalResults = intersection(copy);
	  else if(orClause)
	  finalResults = union(copy);
	 
	 
	}
	
	
	
	
	//end merging 
	
	
	if(!finalResults.isEmpty()){
	
	//System.out.println("Found Some final results .. ");
	
	//get selectedResults 
	//send to sort 
	
	JSONObject jfinal , jtemp ; 
	jfinal = new JSONObject();
	JSONArray displayResults = new JSONArray() ; 
	String[] cols  = null ;  
	boolean columns = false ; 
	String v ; 
	if(selectString.contains(",")){
	cols = selectString.split(",");
	columns = true ; 
	}
	 
	boolean match = false ; 
	
	for( Object objF : finalResults){
	match  = false ; 
	jtemp = (JSONObject)objF ;
	if(!jtemp.isEmpty()){
	 if(star){
	 displayResults.add(jtemp);
	 }
	 else if(cols!=null){
	 for(String c:cols){
	 if(jtemp.get(c.trim())!=null){
	 v = jtemp.get(c.trim()).toString(); 
	 jfinal.put(c.trim(), v);  
	  
	 }
	 
	 }
	 
	 if(!jfinal.isEmpty())
	 displayResults.add(jfinal); 
	 }
	 
	  
	
	}
	
	
	} // end for 
	 
	
	if(!displayResults.isEmpty()){
	
	HashMap<String,String> schemaMap = getSchemaMap(fromString); 
	
	if(!orderByString.equals("")){
	sortResults(displayResults, schemaMap, orderByString);
	}
	
	else{
	//display results locally 
	 JSONObject jf ;
	 for(Object o:displayResults){
	 	jf = (JSONObject)o ; 
	Set set = jf.keySet(); 
	System.out.println();
	for(Object os : set){
	System.out.print("\t| " + jf.get(os.toString()));
	}
	}
	}
	
	}
	
	
	
	
	}
	
	 
	} // end AND OR 
	
	
	
	
	
	
	else System.out.println("Where clauses returned empty.");
	
	
	
	} catch (IOException | ParseException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
	}
	
	}
	}
	
	
	}
	
}
 

private static String searchCategory(String tableName, String whereString) {
	String returnStr = "";
	
	String op = ""; 
	if(whereString.contains("="))op = "=" ;
	else if(whereString.contains(">"))op = ">" ;
	else if(whereString.contains("<"))op = "<" ;
	
	String col = whereString.split(op)[0].trim(); 
	long  recid;
	String fileName = tableName.trim() + "_bt_" + col ; 
	try {
	recid = recman.getNamedObject( fileName );
	if ( recid != 0 ) {
	System.out.println("B+ Tree File found!");
	returnStr = fileName ; 
	}
	else ; //System.out.println("B+tree not found for column.");
	} catch (IOException e) {
	System.out.println("Exception in finding btree file in search category.");
	}
	
	
	return returnStr; 
}

private static void selectJoin(String selectString, String fromString, ArrayList<String> whereClauses,String orderBy,String andOr){
	
	//Select state, cid from complaint, temp where cid=compId and state='CA' order by state 
	
	String smallTable = fromString.split(",")[1].trim();
	String bigTable = fromString.split(",")[0].trim();
	
	//System.out.println("Inside Join - Small Table  " + smallTable + " Big Table " + bigTable);
	
	
	HashMap<String,String> schemaSmall = getSchemaMap(smallTable); 
	HashMap<String,String> schemaBig =	 getSchemaMap(bigTable); 
	
	
	
	boolean secondOnSmall = false ; 
	boolean secondOnBig = false ; 
	boolean second = false ; 
	String operator = ""; 
	String col = "";
	String secondWhereCl = "";
	
	String firstWhereCl = whereClauses.get(0);
	if(whereClauses.size()==2){
	secondWhereCl = whereClauses.get(1);
	second = true ; 
	}
	for(String s : whereClauses){
	 
	 
	if(s.contains("=")){
	col = s.split("=")[0].trim();
	operator = "="; 
	}
	else if(s.contains("<")){
	col = s.split("<")[0].trim();
	operator = "<"; 
	}
	
	else if(s.contains(">")){
	col = s.split(">")[0].trim();
	operator = ">"; 
	}
	
	if(schemaSmall.containsKey(col)&&second){
	secondOnSmall = true ; 
	secondWhereCl = s ; 
	}
	
	else if(schemaBig.containsKey(col)&&second){
	secondOnBig = true ; 
	secondWhereCl = s ; 
	}
	
	 
	}
 
	
	
	String fNameSmall = smallTable + ".json"; 
	 
	
	File f1 = new File(fNameSmall); 
	 
	JSONParser parser = new JSONParser();
	JSONArray resultsSmall = new JSONArray(); 
	 
	if(f1.exists()&&f1.length()!=0){
	 
	Object ob;
	try {
	ob = parser.parse(new FileReader(fNameSmall));
	resultsSmall = (JSONArray)ob;
	//System.out.println("Small table size - " + resultsSmall.size());
	 
	} catch (IOException | ParseException e) {
	
	e.printStackTrace();
	}
	
	}
	int sizeSmall = resultsSmall.size();
	
	//list of small indices 
	ArrayList<Integer> smallInd = new ArrayList<Integer>(); 
	
	for(int si = 0 ; si<=sizeSmall-1;si++){
	smallInd.add(si); 
	}
	
	if(secondOnSmall){
	//System.out.println("Second where on small..");
	resultsSmall = searchFile(smallTable, secondWhereCl, resultsSmall); 
	//System.out.println("Small results from file - " + resultsSmall.size() + " || \n" + resultsSmall);
	JSONObject jsmall ; Set set ; int smallIndex ; String tempS="";
	String pkey = "pid_" + smallTable; 
	ArrayList<Integer> listNewSmall= new ArrayList<Integer>(); 
	for(Object osmall:resultsSmall){
	jsmall = (JSONObject)osmall ;
	if(jsmall.get(pkey)!=null){
	tempS = (String)jsmall.get(pkey);
	smallIndex = Integer.valueOf(tempS);
	listNewSmall.add(smallIndex);
	}
	}
	
	smallInd = listNewSmall ; 
	}
	
	
	//open big file - get results from small list 
	
	String fNameBig = bigTable + ".json"; 
	File f2 = new File(fNameBig); 
	 
	 
	JSONArray resultsBig = new JSONArray(); 
	JSONArray tempBig = new JSONArray(); 
	JSONArray tempTwoBig = new JSONArray();
	
	if(f2.exists()&&f2.length()!=0){
	 
	Object ob;
	try {
	ob = parser.parse(new FileReader(fNameBig));
	tempBig = (JSONArray)ob;
	 
	 
	} catch (IOException | ParseException e) {
	
	e.printStackTrace();
	}
	 
	}
	JSONObject jbtemp = new JSONObject() ; 
	Object otemp ; 
	for(int i:smallInd){
	//System.out.println("Index from small - " + i);
	otemp = tempBig.get(i);
	if(otemp!=null){
	jbtemp = (JSONObject)otemp ; 
	tempTwoBig.add(jbtemp);
	}
	 
	}
	//System.out.println("Temp Big condensed - " + tempTwoBig.size());
	
	if(secondOnBig){ 
	//System.out.println("Second Where Clause - " + secondWhereCl);
	resultsBig = searchFile(bigTable, secondWhereCl, tempTwoBig);
	//System.out.println("After more filtering  - " + resultsBig.size());
	}
	
	else resultsBig = tempTwoBig ; 
	
	
	//start merging 
	
	
	ArrayList<String> selectCols = new ArrayList<String>(); 
	boolean star = false ; 
	if(selectString.contains(",")){
	String cols[] = selectString.split(","); 
	for(String s : cols){
	selectCols.add(s); 
	}
	}
	
	else if(selectString.equals("*")){
	star  = true ; 
	}

	
	JSONArray resultsFinal = new JSONArray(); 
	JSONObject jFinal = new JSONObject(); 
	JSONObject jSmall = new JSONObject(); 
	JSONObject jBig = new JSONObject(); 
	String pidSmall =""; 
	String pidBig = ""; 
	
	String primIndexSmall = "pid_" + smallTable;
	String primIndexBig = "pid_" + bigTable;
	
	
	for(Object osmall : resultsSmall){
	jFinal = new JSONObject(); 
	jSmall = (JSONObject)osmall ; 
	pidSmall = (String) jSmall.get(primIndexSmall); 
	for(Object obig : resultsBig){
	jBig = (JSONObject) obig ; 
	pidBig = (String) jBig.get(primIndexBig);
	if(pidSmall.equals(pidBig)){
	//System.out.println("Match found in merge..");
	if(!star){
	//System.out.println("Inside selected cols");
	for(String c :selectCols){
	
	//System.out.println("Col - " + c + " Value - " + jBig.get(c));
	c =  c.trim();
	if(schemaSmall.get(c)!=null){
	jFinal.put(c,jSmall.get(c)); 
	
	}
	
	if(schemaBig.get(c)!=null){
	jFinal.put(c,jBig.get(c)); 
	}
	}
	
	//System.out.println("Final object - " + jFinal);
	resultsFinal.add(jFinal);
	}
	
	else if(star){
	Set setSmall = jSmall.keySet(); 
	Set setBig = jBig.keySet(); 
	 for(Object o : setSmall){
	  if(jSmall.get(o.toString())!=null)
	  jFinal.put(o.toString(), jSmall.get(o.toString())); 
	  
	 	}
	 
	 for(Object o : setBig){
	 if(jBig.get(o.toString())!=null)
	 jFinal.put(o.toString(), jBig.get(o.toString())); 
	  
	 	}
	 resultsFinal.add(jFinal);
	}
	
	 
	} //end object match 
	}
	
	} //end join 
	
	for (Map.Entry<String, String> entry : schemaSmall.entrySet()) {
	schemaBig.put(entry.getKey(), entry.getValue());
	}

	
if(!orderBy.equals("")){
	sortResults(resultsFinal, schemaBig,orderBy);
}

else{
	 JSONObject jf ;
	 for(Object o:resultsFinal){
	 	jf = (JSONObject)o ; 
	Set set = jf.keySet(); 
	System.out.println();
	for(Object os : set){
	System.out.print("\t| " + jf.get(os.toString()));
	}
	}
}
	
	
	
	
	
	
	
	
	
	
	
}





private static ArrayList<Integer> searchBTree(String fileName , String whereString) {
	 ArrayList<Integer> results = new ArrayList<Integer>();  
	 BTree tree ; 
	 Tuple         tuple = new Tuple();
	 TupleBrowser  browser;
	  
	 int j = 0 ; 
	 long recid;
	 String op = ""; 
	 String col = "";
	 String val = "";
	 int v = 0 ; 
	 
	 if(whereString.contains("=")){
	 op = "=" ; 
	 col = whereString.split("=")[0].trim();
	 val = whereString.split("=")[1].trim();
	 }
	 else if(whereString.contains(">")){
	 op = ">"; 
	 col = whereString.split(">")[0].trim();
	 val = whereString.split(">")[1].trim();
	 }
	 else if(whereString.contains("<")){
	 op = "<";
	 col = whereString.split("<")[0].trim();
	 val = whereString.split("<")[1].trim();
	 }
	 
	 
	  
	try {
	v  = Integer.valueOf(val);
	recid = recman.getNamedObject( fileName );
	 if ( recid != 0 ) {
	         tree = BTree.load( recman, recid );
	         System.out.println( "Reloaded existing B+Tree  " + fileName + " "+ tree.size());
	         if(op.equals("=")){
	        	 if(tree.find(v)!=null){
	        	 results.add((Integer)tree.find(v));
	        	 }
	         }
	         else if(op.equals(">")){
	        	  
	        	 if(tree.find(v)!=null){
	        	  browser = tree.browse(v);
	        	  browser.getNext(tuple); 
	        	  int i ; 
	        	  while(browser.getNext(tuple)){
	        	  i = (Integer) tuple.getValue();
	        	  results.add(i);
	        	  }
	        	 }
	         }
	         
	         else if(op.equals("<")){
	        	  
	        	 if(tree.find(val)!=null){
	        	  browser = tree.browse(val);
	        	  browser.getPrevious(tuple); 
	        	  int i ; 
	        	  while(browser.getPrevious(tuple)){
	        	  i = (Integer) tuple.getValue();
	        	  results.add(i);
	        	  }
	        	 }
	         }
	        
	        	 
	         }
	         
	      
	} catch (IOException e) {
	 
	System.out.println("BTree file error for - " + fileName);
	}
    
	   
	 return results ; 
}

private static JSONArray intersection(ArrayList<JSONArray> inputList) {
	JSONArray results = new JSONArray(); 
	
	int numberOfLists = inputList.size(); 
	JSONArray temp = new JSONArray(); 
	JSONObject j  ; 
	HashMap<JSONObject,Integer> resultsMap = new HashMap<JSONObject,Integer>(); 
	if(numberOfLists==2){
	temp = inputList.get(0); 
	for(int i = 0 ; i<temp.size() ; i++){
	j = (JSONObject)temp.get(i); 
	resultsMap.put(j,0); 
	
	}
	temp = inputList.get(1); 
	for(int i = 0 ; i<temp.size() ; i++){
	j = (JSONObject)temp.get(i); 
	if(resultsMap.containsKey(j)){
	resultsMap.put(j,1); 
	}
	
	}
	
	  
	ArrayList<JSONObject> removeThese = new ArrayList<JSONObject>(); 
	for (Map.Entry<JSONObject, Integer> entry : resultsMap.entrySet()) {
	if(entry.getValue()==0){
	removeThese.add(entry.getKey()); 
	}
	
	}
	
	for(JSONObject jobj:removeThese){
	resultsMap.remove(jobj);
	}
	}
	 
	Set set = resultsMap.keySet(); 
	JSONObject jset = new JSONObject(); 
	for(Object o : set){
	jset = (JSONObject)o ; 
	results.add(jset);
	}
	
	//System.out.println("Results returned from intersection - " + results.size());
	return results; 
}

private static JSONArray union(ArrayList<JSONArray> inputList) {
	
	
	int numberOfLists = inputList.size(); 
	JSONArray temp = new JSONArray(); 
	JSONObject j  ; 
	 
	HashSet<JSONObject> resultSet = new HashSet<JSONObject>();
	
	if(numberOfLists==2){
	temp = inputList.get(0); 
	for(int i = 0 ; i<temp.size() ; i++){
	j = (JSONObject)temp.get(i); 
	resultSet.add(j);  
	}
	temp = inputList.get(1); 
	for(int i = 0 ; i<temp.size() ; i++){
	j = (JSONObject)temp.get(i); 
	resultSet.add(j);
	
	}
	
	 
	}
	//change here as well 
	 
	JSONArray results = new JSONArray(); 
	 
	
	Set set = resultSet ; 
	JSONObject jset = new JSONObject(); 
	for(Object o : set){
	jset = (JSONObject)o ; 
	results.add(jset);
	}
	//System.out.println("Results returned from union - " + results.size());
	return results; 
}






private static void browseTable() {
	System.out.println("Choose a table to browse : \n" + tables.keySet());
	String table , input ; 
	while(true){
	table = reader.nextLine().trim() ;
	
	if(tables.containsKey(table)){
	
	System.out.println("\n1. Insert a new record -  i "
	+ "\n2. Delete a record using primary key - d"
	+ "\n3. Update a record  - u"
	+ "\n4. List records - l"
	+ "\n5. Search records - s "
	+ "\n6. Main menu - m");
	
	input = reader.nextLine();
	
	switch(input){
	case "i":
	insertIntoTable(table);
	break ; 
	case "d":
	deleteRecord(table);
	break ; 
	case "u":
	updateRecord(table);
	break ; 
	
	case "l":
	listRecords(table);
	break ; 
	case "s":
	searchRecords(table);
	case "m":
	break;
	default:
	break; 
	}
	
	break ; 
	
	}
	
	else if(table.equals("#*"))
	break;
	
	else{
	System.out.println("Table does not exsist. "
	+ "Please pick a table from the list above.");
	System.out.println("Or Enter #* to go to main menu.");
	}
	}
	
	}




private static void deleteTable() {
	String deleteTable = "";
	System.out.println("Select a table to delete from the list below - ");
	System.out.println(tables.keySet().toString());
	
	while(true){
	deleteTable = reader.nextLine().trim(); 
	if(tables.containsKey(deleteTable)){
	tables.remove(deleteTable);
	//System.out.println("Table deleted! " + deleteTable);
	String fileName = deleteTable + ".json";
	String fileNameDeleted = deleteTable + "_deleted.json";
	File f = new File(fileName);
	File fDel = new File(fileNameDeleted);
	if(f.exists()){
	//System.out.println("Found file - " + f.getName() + " " + f.getPath()
	//);
	f.delete();
	//f.renameTo(fDel);
	}
	
	
	//System.out.println("Table deleted.");
	System.out.println(tables.keySet());
	break;
	}
	
	else if(deleteTable.equals("#*"))
	break ; 
	
	else{
	System.out.println("Table does not exsist. "
	+ "Please enter a table from the list above.");
	System.out.println("Or Enter #* to go to main menu.");
	}
	}
	
	}




private static void listRecords(String table) {
	
	JSONParser parser = new JSONParser(); 
	JSONArray jsonArray;
	ArrayList<JSONObject> resultList = new ArrayList<JSONObject>();
	String fileName = table + ".json";
	Set kv ; 
	
	
	JSONObject jobj ; 
	String schema = tables.get(table); 
	HashMap<String, String> schemaMap = getSchemaMap(table);
	
	
	 
	System.out.println("Enter column names (separated by comma) to list. Eg movie name,year. Enter #a to return all columns.");
	System.out.println(schema);
	
	
	String inCols = reader.nextLine().trim();
	String input [];
	ArrayList<String> cols = new ArrayList<String>();
	 
	
	
	//select few 
	if(!inCols.equals("#a")){
	if(inCols.contains(",")){
	input = inCols.split(",");
	for(String s : input){
	cols.add(s.trim());
	}
	}
	
	else cols.add(inCols.trim());
	}
	//select all 
	else{
	String[] c = schema.split(","); 
	String schemaCols ; 
	for(String str : c){  
	schemaCols = str.split(":")[0].trim(); 
	cols.add(schemaCols);
	
	}
	}
	
	File file = new File(fileName);
	try {
	if(file.length()>0){
	jsonArray = (JSONArray)parser.parse(new FileReader(fileName));
	Iterator<JSONObject> it = jsonArray.iterator();
	 
	JSONObject resultObject ;
	for(int i = 0 ; i<jsonArray.size();i++){
	    jobj = (JSONObject)jsonArray.get(i);
	    resultObject = new JSONObject();
	    //resultObject = jobj;
	kv = jobj.keySet(); 
	System.out.println();
	
	 for(Object o : kv){
	  
	if(cols.contains(o.toString())){
	 	System.out.print("\t\t| " + jobj.get(o.toString()));
	resultObject.put(o.toString(), jobj.get(o.toString()));
	
	
	}
	 }
	 resultList.add(resultObject);
	 
	}
	}
	System.out.println("\n\nResults returned = " + resultList.size());
	
	//if(resultList.size()>0)
	//sortResults(resultList,schemaMap,"");
	
	}
	catch(Exception e){
	e.printStackTrace();
	}
	
	 
	}




private static HashMap<String, String> getSchemaMap(String table) {
	
	HashMap<String,String> schemaMap = new HashMap<String,String>(); 
	String schema = tables.get(table); 
	String schemaCols[] = schema.split(",");
	for(String s:schemaCols){
	if(s.contains(":")){
	schemaMap.put(s.split(":")[0], s.split(":")[1]);
	}
	}
	
	
	
	return schemaMap;
}
 
private static void sortResults(JSONArray result, HashMap<String,String> schemaMap, String orderBy) {
	
	    ArrayList<JSONObject> resultList = result ; 
	String col = "";
	String or = "a"; 
	if(!orderBy.equals("")){
	//System.out.println("inside order by");
	if(orderBy.toLowerCase().contains("desc")
	||orderBy.toLowerCase().contains("asc")) {
	
	if(orderBy.toLowerCase().contains("desc")){
	//System.out.println("inside desc");
	col = orderBy.split("desc")[0].trim();
	or = "d";
	}
	
	else if(orderBy.toLowerCase().contains("asc")) {
	col = orderBy.split("asc")[0].trim();
	or = "a";
	}
	
	}
	 
	else {
	col = orderBy.trim(); 
	}
	
	}
	
	
	
	else {
	System.out.println("Order by column not identifed.");
	return ; 
	}
	
	//System.out.println("Order by - " + col);
	final String sortBy = col ; 
	   
	String type = schemaMap.get(sortBy.trim()); 
	  
	final String order = or ;  
	
	//System.out.println("Result Size in sort - " + resultList.size());
	
	
	if(order.equals("a")||order.equals("d")){
	try{
	Collections.sort(resultList,new Comparator<JSONObject>() {
	 public int compare(JSONObject o1, JSONObject o2) {
	 
	String one = (String)o1.get(sortBy);
	String two = (String)o2.get(sortBy);
	 	if(one.compareTo(two)>0){
	if(order.equals("d"))
	return -1;
	else return 1;
	}
	 
	else if(one.compareTo(two)==0)
	return 0; 
	  
	else {
	if(order.equals("d"))
	return 1;
	else return -1;
	}
	 
	 
	}
	 
	});
	 
	 JSONObject j ; 
	 for(Object o:resultList){
	 	j = (JSONObject)o ;
	 	if(!j.isEmpty()){
	 	Set set = j.keySet(); 
	System.out.println();
	for(Object ob : set){
	System.out.print("\t| " + j.get(ob.toString()));
	}
	 	}
	
	}
	}catch(Exception e){
	System.out.println("Something went wrong - check query and try again.");
	}
	 
	
	}
	
}

 
private static void updateRecord(String table) {
	String fName = table + ".json"; 
	System.out.println(tables.get(table));
	System.out.println("\nEnter column values separated by comma. ");
	
	reader = new Scanner(System.in); 
	String s = reader.nextLine().trim();
	String schema[] = tables.get(table).split(",");
	String values[] = s.split(",");
	JSONObject obj = new JSONObject();
	String primKey ="" ; 
	String primKeyValue = "";
	 
	for(int i=0; i < schema.length ; i++){
	if(schema[i].contains("#p")){
	schema[i] = schema[i].split("#p")[0].split(":")[0].trim();
	primKey = schema[i];
	primKeyValue = s.split(",")[i].trim();
	
	}
	
	else schema[i] = schema[i].split(":")[0].trim();
	try{
	obj.put(schema[i].trim(),s.split(",")[i].trim());
	}
	catch(Exception e){
	System.out.println("Invalid input!");
	break ; 
	}
	
	}
	
	
	JSONParser parser = new JSONParser();
	 
	JSONArray jsonArray,jArrayCopy;
	JSONObject jobject; 
	try {
	File f = new File(fName); 
	if(f.exists()&&f.length()!=0){
	 
	Object ob = parser.parse(new FileReader(fName));
	jsonArray = (JSONArray)ob;
	jArrayCopy  = jsonArray;
	boolean keyExsists = false ;
	
	 
	keyExsists = false ; 
	for(int i = 0 ; i<jsonArray.size();i++){
	    jobject = (JSONObject)jsonArray.get(i);
	Set keySet = jobject.keySet(); 
	for(Object o:keySet){
	if(o.toString().equals(primKey)){
	if(jobject.get(primKey).equals(primKeyValue)){
	System.out.println(primKey + " = " + primKeyValue);
	jArrayCopy.remove(i); 
	keyExsists = true ;
	}
	}
	}
	 }
	if(keyExsists){
	jArrayCopy.add(obj);
	f.delete();
	f.createNewFile();
	FileOutputStream fos = new FileOutputStream(fName);
	fos.write(jArrayCopy.toJSONString().getBytes());
	fos.flush();
	fos.close();
	System.out.println("1 Row updated!");
	 }
	 	 
	else{
	
	System.out.println("No entry with the specfied primary key.");
	
	}
	
	
	}
	else if(f.exists()&&f.length()==0)
	System.out.println("Table is empty!");
	}catch(Exception e){
	e.printStackTrace();
	}
	
	
	}


private static void deleteRecord(String table) {
	String fName = table + ".json"; 
	System.out.println("\nEnter primary key to delete.");
	
	reader = new Scanner(System.in); 
	
	String primKeyValue = reader.nextLine().trim();
	
	String schema[] = tables.get(table).split(",");
	
	JSONObject obj = new JSONObject();
	String primKey ="" ; 
	 
	
	for(int i=0; i < schema.length ; i++){
	if(schema[i].contains("#p")){
	schema[i] = schema[i].split("#p")[0].split(":")[0].trim();
	primKey = schema[i];
	
	}
	 
	}
	
	JSONParser parser = new JSONParser();
	 
	JSONArray jsonArray, jArrayCopy;
	JSONObject jobject; 
	 
	File f = new File(fName); 
	if(f.exists()&&f.length()!=0){
	 
	Object ob;
	try {
	ob = parser.parse(new FileReader(fName));
	jsonArray = (JSONArray)ob;
	jArrayCopy = jsonArray;
	boolean keyExsists = false ;
	 
	for(int i = 0 ; i<jsonArray.size();i++){
	    jobject = (JSONObject)jsonArray.get(i);
	Set keySet = jobject.keySet(); 
	for(Object o:keySet){
	if(o.toString().equals(primKey)){
	if(jobject.get(primKey).equals(primKeyValue)){
	
	System.out.println(primKey + " = " + primKeyValue);
	jArrayCopy.remove(i); 
	keyExsists = true ;
	}
	}
	}
	}
	 
	if(keyExsists){
	f.delete();
	f.createNewFile();
	FileOutputStream fos = new FileOutputStream(fName);
	fos.write(jArrayCopy.toJSONString().getBytes());
	fos.flush();
	fos.close();
	System.out.println("1 Row deleted.");
	}
	else{
	System.out.println("Entry with the primary key does not exsist.");
	}
	
	
	} catch (IOException | ParseException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
	}
	
	
	}
	}

private static void insertIntoTable(String qs) {
	//Extracting table name , Btree , file_name from qs
	String table_name = qs.split("\\s")[2].trim() ;
	String Btree_name = table_name.concat("_bt_pid");
	String file_name = table_name.concat(".json");
	
	// creating an object for corresponding table file
	File table_file = new File(file_name);
	
	//check 1 , if insertion is done before creation alert the user
	if(!table_file.exists()){
		// if file is not there that means the table has not been created so insertion operation is not possible'
		System.out.println(table_name + " does not exist");
		return ;
	}
	
	// Extracting the json index to update the primary key
	Integer jIndex = 0 ;
	JSONParser parser = new JSONParser();
	JSONArray jArray = new JSONArray() ;
	JSONObject jObject = new JSONObject() ;
	
	//Reading the jsonArray from corresponding table file
	try {
		if(table_file.length() != 0){
		Object ob = parser.parse(new FileReader(table_file));
		jArray = (JSONArray)ob ;
		jIndex = jArray.size() ;
		
		}
	} catch (IOException | ParseException e) {
		// TODO Auto-generated catch block
		System.out.println("Error while reading the file inside the insertIntoTable function ");
		e.printStackTrace();
	}
	// JSON object formation for insertion into table_file and btree
	
	String attri_string = qs.split("\\s")[3].replace("(","").replace(")","");
	String[] attri_array = attri_string.split(",");
	
	String val_string = qs.split("\\s")[5].replace("(","").replace(")","");
	String[] val_array = val_string.split(",") ;
	
	// Check 2 , if the size of the attri_array is not equal to size of the val_array then query is incorrect 
	if(attri_array.length != val_array.length){
		System.out.println("Invalid query, number of colums passed is not equal to no of values");
		return ;
	}
	
	// Forming the object to insert into the json array 
	
	String pk = "pid_".concat(table_name);
	jObject.put(pk, jIndex.toString());
	
	for(int i = 0 ; i<val_array.length ; i++){
		jObject.put(attri_array[i], val_array[i]);
	}
		
	// Updating the JSON File
	jArray.add(jObject) ;
	
	table_file.delete() ;
	try {
		table_file.createNewFile() ;
		FileOutputStream fos = new FileOutputStream(file_name);
		fos.write(jArray.toJSONString().getBytes());
		fos.flush();
		fos.close();
		System.out.println("1 Row Inserted in "+table_name);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		System.out.println("Error while inserting the record in corresponding json file , inside insertIntoTable");
		e.printStackTrace();
	}
	
	// Updating the corresponding Btree file
	
	try {
		 long recid = recman.getNamedObject(Btree_name);
		 BTree tree ;
			
			if(recid != 0){ // B+tree already exists
				tree = BTree.load(recman, recid);
				System.out.println("B+Tree size before update "+tree.size());
				tree.insert(jIndex, jIndex, false);
				// commiting in the btree
				recman.commit();
				System.out.println("B+Tree Update, the size of B+Tree is now "+ tree.size());
			
			}
			else{
				tree = BTree.createInstance(recman, new IntegerComparator());
				recman.setNamedObject(Btree_name, tree.getRecid());
				tree.insert(jIndex, jIndex, false);
				// commiting in the btree
				recman.commit();
				System.out.println("Created a new B+tree, the size of B+Tree is now "+tree.size());
			}
			
	} catch (IOException e) {
		// TODO Auto-generated catch block
		System.out.println("Unable to get the instance of corresponding BTree inside insertIntoTable");
		e.printStackTrace();
	}
		
}

	
private static void searchRecords(String table) {
	
	String fName = table + ".json"; 
	System.out.println(tables.get(table));
	
	System.out.println("\nEnter column name to search.");
	String col = reader.nextLine().trim();
	
	System.out.println("\nEnter operator - < or = or >");
	String operator = reader.nextLine().trim();
	 
	
	System.out.println("\nEnter value to search");
	String value = reader.nextLine().trim();
	
	String schema[] = tables.get(table).split(",");
	JSONObject obj = new JSONObject();
	String primKey ="" ; 
	HashMap<String, String> schemaMap = getSchemaMap(table);
	ArrayList<JSONObject> resultList = new ArrayList<JSONObject>();
	ArrayList<String> integerCols = new ArrayList<String>();
	String intCol ; 
	for(String s : schema){
	if(s.split(":")[1].contains("int")){
	if(s.contains("#p")){
	intCol = s.split("#p")[0].split(":")[0].trim();
	}
	else intCol = s.split(":")[0].trim();
	integerCols.add(intCol);
	}
	}
	JSONParser parser = new JSONParser();
	 
	JSONArray jsonArray,jArrayCopy, resultsArray;
	resultsArray = new JSONArray();
	JSONObject jobject; 
	try {
	File f = new File(fName); 
	if(f.exists()&&f.length()!=0){
	 
	Object ob = parser.parse(new FileReader(fName));
	jsonArray = (JSONArray)ob;
	jArrayCopy  = jsonArray;
	String c = "";
	String v = "";
	boolean match = false; 
	int compareCase =0 ; 
	
	if(operator.equals("=")) compareCase = 1 ; 
	if(operator.equals("<")) compareCase = 2 ; 
	if(operator.equals(">")) compareCase = 3 ; 
	
	int comp = 0;
	
	for(int i = 0 ; i<jsonArray.size();i++){
	match = false ; 
	    jobject = (JSONObject)jsonArray.get(i);
	Set keySet = jobject.keySet(); 
	for(Object o:keySet){
	c = o.toString();  
	if(c.equals(col)) {
	v = jobject.get(o.toString()).toString();
	//System.out.println("\nCol = " + c + " Value = " + v);
	comp = v.compareTo(value); 
	if(v.length()>value.length()) comp = 10; 
	else if(v.length()<value.length())comp = -10;
	if(
	(compareCase==1&&comp==0)
	||(compareCase==2&&comp<0)
	||(compareCase==3&&comp>0)
	) {
	match = true ;
	
	}
	 
	}
	}
	 	  	if(match){
	 	  	for(Object oCopy:keySet){
	System.out.print("\t| " + jobject.get(oCopy.toString()));
	 	  	}
	 	  	resultList.add(jobject);
	 	  	System.out.println();	
	 	  	}
	
	 
	}
	
	}
	
	System.out.println("Results returned = " + resultList.size());
	//if(resultList.size()>0)
	//sortResults(resultList, schemaMap, "");
	 
	}
	
	catch(Exception e){
	e.printStackTrace();
	}
	
	
	
	}

private static JSONArray searchFile(String table,String whereString,JSONArray inputArray){
	
	//ArrayList<JSONObject> resultList = new ArrayList<JSONObject>();
	JSONArray resultArray = new JSONArray();

	int compareCase = 0 ; 
	int comp = 0 ; 
	String operator = "";
	if(whereString.contains("=")) {
	compareCase = 1 ; 
	operator = "=";
	}
	
	
	if(whereString.contains("<")) {
	compareCase = 2 ; 
	operator = "<";
	}
	if(whereString.contains(">")) {
	compareCase = 3 ; 
	operator = ">";
	}
	boolean match = false ; 
	JSONObject jobject ; 
	String column = "" , value = ""; 
	boolean noWhere = false ; 
	if(!whereString.trim().equals("")){
	column = whereString.split(operator)[0].trim();
	 value =  whereString.split(operator)[1].trim();
	  
	}
	else noWhere = true ; 
	
	if(noWhere){
	//return input Array 
	return inputArray ; 
	}
	
	String c = "";
	String v = "";
	
	for(int i = 0 ; i<inputArray.size();i++){
	match  = false ; 
	jobject = (JSONObject)inputArray.get(i);
	Set keySet = jobject.keySet(); 
	for(Object o:keySet){
	c = o.toString().trim();
	if(column.equals(c)){
	v = jobject.get(o.toString()).toString();
	comp = v.compareTo(value); 
	if(v.length()>value.length()) comp = 10; 
	else if(v.length()<value.length())comp = -10;
	if(
	(compareCase==1&&comp==0)
	||(compareCase==2&&comp<0)
	||(compareCase==3&&comp>0)
	) {
	match = true ;
	break ; 
	}
	 
	}
	
	}
	
	if(match){
	resultArray.add(jobject);
	 
	}
	}
	 
	
	return resultArray ; 
	 
	
}
	 
private static void createNewTable() {
	System.out.println("Enter table name :");
	String table;
	while(true){
	 table = reader.nextLine().trim();
	 if(!tables.containsKey(table))
	 break ;
	 else System.out.println("Table already exists. Enter new table name :");
	}
	 
	System.out.println("Table to create - " + table);
	
	System.out.println("Enter column names and attribute type separated by comma. "
	+ "\nEg - id:int#p , Movie Name : string"
	+ "\nAvailable attribute types are - string , int and float.");
	
	String cols;
	cols = reader.nextLine().trim();
	String pid = "pid_" + table + ":int,"; 
	cols = pid + cols ; 
	
	 
	tables.put(table,cols);
	
	try{
	String newTableFile = table + ".json" ; 
	File file = new File(newTableFile);
	file.createNewFile();
	System.out.println("Table created.");
	 
	 }
	catch(IOException ioe){
	ioe.printStackTrace();
	}
	
	}

private static void writeHashMaptoFile() {
	try{
	FileOutputStream fos = new FileOutputStream("tableSchema.ser") ;
	ObjectOutputStream oos = new ObjectOutputStream(fos);
	oos.writeObject(tables);
	oos.close();
	fos.close();
	}
	catch(IOException ioe){
	ioe.printStackTrace();
	} 
	 
	}

private static void loadTable() throws FileNotFoundException { 
	try {
	FileInputStream fileInputStream = new FileInputStream("tableSchema.ser");
	ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
	tables = (HashMap<String, String>) objectInputStream.readObject();
	System.out.println("Tables loaded - " + tables.size());
	} catch (FileNotFoundException e) {
	 System.out.println("DB does not exist .. ");
	 writeHashMaptoFile();
	 System.out.println("DB created!");
	}
	
	
	  catch (IOException e) {
	 
	e.printStackTrace();
	} catch (ClassNotFoundException e) {
	 
	e.printStackTrace();
	} 
	
	} 

	 
} 