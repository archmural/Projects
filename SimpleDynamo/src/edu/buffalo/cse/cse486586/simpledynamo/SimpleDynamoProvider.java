package edu.buffalo.cse.cse486586.simpledynamo;
//SimpleDynamoProvider (5 avds-Phase 4)
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;

import org.w3c.dom.Node;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.text.TextUtils;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	//for 5 avds
	static final String nodeList[]=new String[5];
	//for 3 avds
	//static final String nodeList[]=new String[3];
    private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
    static final int SERVER_PORT = 10000;
    private DatabaseHelper mOpenHelper;
    private static final String DBNAME = "mydb";
    private SQLiteDatabase db;
    private String node_id="";
    private String myPort="";
    private String portStr="";
    private String successor1="";
    private String successor2="";
    private String predecessor1="";
    private String predecessor2="";
    private Cursor finalQueryResult=null;
    private Cursor finalGDumpResult=null;
    private int deletedRows=0;
    //for 5 avds
    private String gDumpArray[]= new String[5];
    //for 3 avds
    //private String gDumpArray[]= new String[3];
    Object queryMutex=new Object();
    //for 5 avds
    Object gDumpMutex[]=new Object[5];
    //for 3 avds
    //Object gDumpMutex[]=new Object[3];
    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
    	String key = (String) values.get(KEY_FIELD);
		String val = (String) values.get(VALUE_FIELD);
		Log.d(TAG,"Insert request generated at "+ portStr+" Key field="+key+" Value field="+val );
		//logic to send insert request to correct avd and its successors
		String previous="";
		try {
			for(int i=0;i<nodeList.length;i++){
				//for 5 avds
				previous=nodeList[(i+4)%5];
				//for 3 avds
				//previous=nodeList[(i+2)%3];
				Log.d(TAG,"predecessor is="+previous);
				if(i==0){
					if((genHash(key).compareTo(genHash(previous))>0 || (genHash(key).compareTo(genHash(nodeList[i]))<=0))){
						Log.d(TAG,"corner case for insert");
						//can insert in node i
						//for 5 avds
						Log.d(TAG,"Sending to "+nodeList[i]+", "+nodeList[(i+1)%5]+" and "+nodeList[(i+2)%5]);
						//for 3 avds
						
						//Log.d(TAG,"Sending to "+nodeList[i]+", "+nodeList[(i+1)%3]+" and "+nodeList[(i+2)%3]);
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"INSERT_REQUEST",key,val,portStr,convertToPort(nodeList[i]),null,null,null);
						//send to i's next two successors
						//for 5 avds
						Log.d(TAG,"Returned after calling 1st client task");
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"INSERT_REQUEST",key,val,portStr,convertToPort(nodeList[(i+1)%5]),null,null,null);
						Log.d(TAG,"Returned after calling 2nd client task");
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"INSERT_REQUEST",key,val,portStr,convertToPort(nodeList[(i+2)%5]),null,null,null);
						
						//for 3 avds
						//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"INSERT_REQUEST",key,val,portStr,convertToPort(nodeList[(i+1)%3]),null,null,null);
						
						//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"INSERT_REQUEST",key,val,portStr,convertToPort(nodeList[(i+2)%3]),null,null,null);
						Log.d(TAG,"Returned after calling third client task");
						//break;
					}				
				}
				else{
					if((genHash(key).compareTo(genHash(previous))>0 && (genHash(key).compareTo(genHash(nodeList[i]))<=0))){
						//can insert in node i
						Log.d(TAG,"Normal case for insert");
						//for 5 avds
						Log.d(TAG,"Sending to "+nodeList[i]+", "+nodeList[(i+1)%5]+" and "+nodeList[(i+2)%5]);
						//for 3 avds
						
						//Log.d(TAG,"Sending to "+nodeList[i]+", "+nodeList[(i+1)%3]+" and "+nodeList[(i+2)%3]);
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"INSERT_REQUEST",key,val,portStr,convertToPort(nodeList[i]),null,null,null);
						//send to i's next two successors
						Log.d(TAG,"Returned after calling first client task");
						//for 5 avds
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"INSERT_REQUEST",key,val,portStr,convertToPort(nodeList[(i+1)%5]),null,null,null);
						Log.d(TAG,"Returned after calling second client task");
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"INSERT_REQUEST",key,val,portStr,convertToPort(nodeList[(i+2)%5]),null,null,null);
						
						//for 3 avds
						
						//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"INSERT_REQUEST",key,val,portStr,convertToPort(nodeList[(i+1)%3]),null,null,null);
						
						//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"INSERT_REQUEST",key,val,portStr,convertToPort(nodeList[(i+2)%3]),null,null,null);
						Log.d(TAG,"Returned after calling third client task");
						//break;
					}
				}
			}
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
        return uri;
    }

    @Override
    public boolean onCreate() {
    	//initialise global array of nodes : 5 AVDS
    	nodeList[0]="5562";//REMOTE_PORT4;
    	nodeList[1]="5556";//REMOTE_PORT1;
    	nodeList[2]="5554";//REMOTE_PORT0;
    	nodeList[3]="5558";//REMOTE_PORT2;
    	nodeList[4]="5560";//REMOTE_PORT3;
    	//for 3 avds
//    	nodeList[0]="5556";
//    	nodeList[1]="5554";
//    	nodeList[2]="5558";
    	//determine running port
    	getMyPort();
    	//determine its successors and predecessors
    	getNetworkInfo();
    	Log.d(TAG,"App started at "+portStr);
    	
    	mOpenHelper = new DatabaseHelper(
                getContext(),        // the application context
                DBNAME,              // the name of the database)
                null,                // uses the default SQLite cursor
                1                    // the version number
            );
    	//Log.d(TAG,"Content initialised flag="+mOpenHelper.contentInitialized);
    	db = mOpenHelper.getWritableDatabase();
    	if(mOpenHelper.contentInitialized){
    		//app restarting,perform recovery
    		Log.d(TAG,"Fresh install..");
    	}else{
    		Log.d(TAG, "Recovery");
    		//calling recovery method
    		getNetworkInfo();
    		new RecoveryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"RECOVERY_REQUEST",null,null,portStr,convertToPort(predecessor1),null,null,null);
    		new RecoveryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"RECOVERY_REQUEST",null,null,portStr,convertToPort(predecessor2),null,null,null);
    		new RecoveryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"RECOVERY_REQUEST",null,null,portStr,convertToPort(successor1),null,null,null);
    	   		
    	}
    	
    	 try {
             
          	ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
          	//Log.d(TAG,"server socket created"+serverSocket);
             new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
          } catch (IOException e) {
             
              Log.e(TAG, "Can't create a ServerSocket");
             
          }
    	 catch(Exception e){
    		 Log.e(TAG, "Some exception while creating server socket in onCreate method");
    		 e.printStackTrace();
    	 }
    	try {
			node_id = genHash(portStr);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return true;
    }
      
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
    	//selection,whereClause is null
    	Log.d(TAG,"selection for delete="+selection);
       	int no=0;
    	if(selection.equals("@"))
    	{
    		//delete all rows from local dht
    		no=db.delete("Dynamo", null, null);
    		deletedRows=no;
       	}
    	else if(selection.equals("*"))
    	{
    		//delete all rows from global dht
    		Log.d(TAG,"inside global delete condition of query method");
    		//for 3 avds
    		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"GLOBAL_DELETE",selection,null,portStr,convertToPort(nodeList[0]),null,null,null);
    		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"GLOBAL_DELETE",selection,null,portStr,convertToPort(nodeList[1]),null,null,null);
    		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"GLOBAL_DELETE",selection,null,portStr,convertToPort(nodeList[2]),null,null,null);
    		//for 5 avds
    		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"GLOBAL_DELETE",selection,null,portStr,convertToPort(nodeList[3]),null,null,null);
    		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"GLOBAL_DELETE",selection,null,portStr,convertToPort(nodeList[4]),null,null,null);
    	}
    	else
    	{
    		//find key and delete
    		String previous="";
    		try {
    			for(int i=0;i<nodeList.length;i++){
    				//for 5 avds
    				previous=nodeList[(i+4)%5];
    				//for 3 avds
    				//previous=nodeList[(i+2)%3];
    				Log.d(TAG,"predecessor is="+previous);
    				if(i==0){
    					if((genHash(selection).compareTo(genHash(previous))>0 || (genHash(selection).compareTo(genHash(nodeList[i]))<=0))){
    						//can delete from node i
    						//for 5 avds
    						Log.d(TAG,"Deleting from "+nodeList[i]+", "+nodeList[(i+1)%5]+" and "+nodeList[(i+2)%5]);
    						//for 3 avds
    						//Log.d(TAG,"Deleting from "+nodeList[i]);
    						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"DELETE_REQUEST",selection,null,portStr,convertToPort(nodeList[i]),null,null,null);
    					    //deleting from i's next two successors
    						//for 5 avds
    						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"DELETE_REQUEST",selection,null,portStr,convertToPort(nodeList[(i+1)%5]),null,null,null);
    						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"DELETE_REQUEST",selection,null,portStr,convertToPort(nodeList[(i+2)%5]),null,null,null);
    						
    						//for 3 avds
    						//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"DELETE_REQUEST",selection,null,portStr,convertToPort(nodeList[(i+1)%3]),null,null,null);
    						//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"DELETE_REQUEST",selection,null,portStr,convertToPort(nodeList[(i+2)%3]),null,null,null);
    					}				
    				}
    				else{
    					if((genHash(selection).compareTo(genHash(previous))>0 && (genHash(selection).compareTo(genHash(nodeList[i]))<=0))){
    						//can delete from node i
    						//for 5 avds
    						Log.d(TAG,"Deleting from "+nodeList[i]+", "+nodeList[(i+1)%5]+" and "+nodeList[(i+2)%5]);
    						//for 3 avds
    						//Log.d(TAG,"Deleting from "+nodeList[i]);
    						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"DELETE_REQUEST",selection,null,portStr,convertToPort(nodeList[i]),null,null,null);
    					    //deleting from i's next two successors
    						//for 5 avds
    						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"DELETE_REQUEST",selection,null,portStr,convertToPort(nodeList[(i+1)%5]),null,null,null);
    						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"DELETE_REQUEST",selection,null,portStr,convertToPort(nodeList[(i+2)%5]),null,null,null);
    						
    						//for 3 avds
    						//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"DELETE_REQUEST",selection,null,portStr,convertToPort(nodeList[(i+1)%3]),null,null,null);
    						//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"DELETE_REQUEST",selection,null,portStr,convertToPort(nodeList[(i+2)%3]),null,null,null);
    					}
    				}
    			}
    			} catch (NoSuchAlgorithmException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}
    		
    	}
    	Log.d(TAG,"No of rows deleted="+deletedRows);
        return deletedRows;
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
       //put a lock so that main thread waits till final cursor is obtained
    	//selection=null to return all rows
    	Log.v("query", selection);
    	
    	if(selection.equals("@"))
    	{
    		//return all rows from local dht
    		Cursor resultCursor=null;
    		Log.d(TAG,"inside ldump condition of query method");
    		resultCursor = db.query("Dynamo", projection,null, selectionArgs, null, null, sortOrder);
    		Log.d(TAG,"No of rows queried="+resultCursor.getCount()+" reslt cursor="+resultCursor);
    		return resultCursor;
    	}
    	else if(selection.equals("*"))
    	{
    		Cursor resultCursor=null;
    		//return all rows from global dht
    		Log.d(TAG,"inside gdump condition of query method");
    		//implementing busy wait
    		//Call the servers of all AVDS and after sending the request, continue reading from the same socket
    		for(int i=0;i<nodeList.length;i++)
    		{
	    		String initiator="";//original sender of message
	        	String key="";
	        	String value="";
	        	String successor="";
	        	String predecessor="";
	        	String destination="";
	        	String queryResult="";
	        	String messageType="";
	            try {
	            	Log.d(TAG,"destination is="+nodeList[i]);
	            	destination= convertToPort(nodeList[i]);
	            	
	            	Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
	                Integer.parseInt(destination));
	            	messageType="GLOBAL_DUMP";
	            	key=null;
	            	value=null;
	            	initiator=portStr;
	            	successor=null;
	            	predecessor=null;
	            	queryResult="";
	               
	                OutputStream outStream = socket.getOutputStream();             
	                PrintWriter pw = new PrintWriter(outStream, true);
	                Log.d(TAG,"Client message to be sent directly ="+messageType+"!"+key+"@"+value+"#"+initiator+"$"+successor+"%"+predecessor+"^"+queryResult);
	                pw.println(messageType+"!"+key+"@"+value+"#"+initiator+"$"+successor+"%"+predecessor+"^"+queryResult);
	                System.out.flush();
	                
	                //continue reading on the same socket
	                InputStream inStream = socket.getInputStream(); 
					BufferedReader fromClient = new BufferedReader(new InputStreamReader(inStream)); 
					String s=fromClient.readLine();
					//received result of local dump from one avd
					Log.d(TAG,"Received result of "+nodeList[i]+"="+s);
					//TODO: check if null string enters this condition
					if(s==null){
						Log.d(TAG,"Response is null");
						continue;
					}
					gDumpArray[i]=s;
					//close everything
					//pw.close();
					//fromClient.close();
	                //socket.close();
	            } catch (UnknownHostException e) {
	            	//TODO: continue the for loop here
	                Log.e(TAG, "ClientTask UnknownHostException");
	            } catch (IOException e) {
	                Log.e(TAG, "ClientTask socket IOException");
	            }	
	            catch(Exception e){
	            	Log.e(TAG, "Socket Timeout Exception in * condition of query method");
	            	e.printStackTrace();
	            	
	            	continue;
	            }
    		}
	    	Log.d(TAG,"Waiting done: Collect global dump results");
			//on receiving all local dumps
    		//put contents in a matrix cursor
	    	//TODO:check contents of each element in gDumpArray to check if empty before concatenating
	    	String queryResult="";
	    	for(int i=0;i<gDumpArray.length;i++){
	    		queryResult+=gDumpArray[i]+"::";
	    	}
	    	Log.d(TAG,"Query result of global dump="+queryResult);
    		String columnNames[]={"key","value"};
    		MatrixCursor result=new MatrixCursor(columnNames);
    		String keyValues[]=queryResult.split("::");
    		Log.d(TAG," keyvalues array="+keyValues.length);
    		for(int j=0;j<keyValues.length;j++)
    			Log.d(TAG," keyvalues array[="+j+"]="+keyValues[j]);
    		int i=0;
    		if(!queryResult.isEmpty()){
    		while(i<keyValues.length)
    		{
    			String temp[]=new String[2];
    			temp[0]=keyValues[i++];
    			temp[1]=keyValues[i];
    			Log.d(TAG,"key value= "+temp[0]+" "+temp[1]);
    			if(i%2!=0)
    			result.addRow(temp);
    			i++;
    		}
    		}
    		else{
    			Log.d(TAG,"Queryresult is zero,return empty cursor");
    		}
    		finalGDumpResult=(Cursor)result;
    		return finalGDumpResult;
    	}
    	else
    	{
    		Log.d(TAG,"inside particular condition of query method");
    		String previous="";
    		String result="";
    		try {
    			for(int i=0;i<nodeList.length;i++){
    				//for 5 avds
    				previous=nodeList[(i+4)%5];
    				//for 3 avds
    				//previous=nodeList[(i+2)%3];
    				Log.d(TAG,"predecessor is="+previous);
    				if(i==0){
    					if((genHash(selection).compareTo(genHash(previous))>0 || (genHash(selection).compareTo(genHash(nodeList[i]))<=0))){
    					//can query from node i
    						Log.d(TAG,"Quering from "+nodeList[i]);
    						result=connectServer("QUERY_REQUEST", selection, null, portStr, nodeList[i], null, null, null);
    					}				
    				}
    				else{
    					if((genHash(selection).compareTo(genHash(previous))>0 && (genHash(selection).compareTo(genHash(nodeList[i]))<=0))){
    					//can query from node i
    						Log.d(TAG,"Quering from "+nodeList[i]);
    						result=connectServer("QUERY_REQUEST", selection, null, portStr, nodeList[i], null, null, null);
    					}
    				}
    			}
    		} catch (NoSuchAlgorithmException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
    		Log.d(TAG,"Query result string for key="+selection+" is ="+result);
    		String columnNames[]={"key","value"};
    		MatrixCursor queryCursor=new MatrixCursor(columnNames);
    		//TODO:check if result is null
    		if(result!=null){
	    		String keyValues[]=result.split("::");
	    		Log.d(TAG," keyvalues array="+keyValues.length);
	    		for(int j=0;j<keyValues.length;j++)
	    			Log.d(TAG,"Query result array[="+j+"]"+keyValues[j]);
	    		int i=0;
	    		while(i<keyValues.length)
	    		{
	    			String temp[]=new String[2];
	    			temp[0]=keyValues[i++];
	    			temp[1]=keyValues[i];
	    			Log.d(TAG,"key value= "+temp[0]+" "+temp[1]);
	    			if(i%2!=0)
	    				queryCursor.addRow(temp);
	    			i++;
	    		}
	    		finalQueryResult=(Cursor)queryCursor;
    		}
    		return finalQueryResult;
    		
    	}
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
       
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
    public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
        	Log.d(TAG, "Inside server");
            ServerSocket serverSocket = sockets[0];
           
            try {
				
				while(true) 
				{ 
					Socket socket = serverSocket.accept( );
					Log.d(TAG, "Connected to client!!");
					
					InputStream inStream = socket.getInputStream(); 
					BufferedReader fromClient = new BufferedReader(new InputStreamReader(inStream)); 
					String msgFromClient="";
					String temp="";
					msgFromClient=fromClient.readLine();
					
					Log.d(TAG, "Inside server,message at "+portStr+"="+msgFromClient);
					//to display message
					if(msgFromClient!=null)
					{
						//check if server is AVD0 and if message is a join request,reply if avd 0 is the answer or pass on the message to other avds
						
						String messageType=msgFromClient.substring(0,msgFromClient.indexOf("!",0));
						String key=msgFromClient.substring(msgFromClient.indexOf("!",0)+1,msgFromClient.indexOf("@",0));
						String value=msgFromClient.substring(msgFromClient.indexOf("@",0)+1,msgFromClient.indexOf("#",0));
						String initiator=msgFromClient.substring(msgFromClient.indexOf("#",0)+1,msgFromClient.indexOf("$",0));
						String successor=msgFromClient.substring(msgFromClient.indexOf("$",0)+1,msgFromClient.indexOf("%",0));
						String predecessor=msgFromClient.substring(msgFromClient.indexOf("%",0)+1,msgFromClient.indexOf("^",0));
						String queryResult=msgFromClient.substring(msgFromClient.indexOf("^",0)+1);
						String result=routeChord(key,value,messageType,initiator,successor,predecessor,queryResult);
						if(messageType.equals("GLOBAL_DUMP") || messageType.equals("QUERY_REQUEST") || messageType.equals("RECOVERY_REQUEST")){
							//write the query result to the socket which is received by waiting socket
							if(result!=null){
								//can combine both cases,separated to check working
								Log.d(TAG,"Result in server task="+result);
								if(!result.isEmpty()){
									OutputStream outStream = socket.getOutputStream();             
					                PrintWriter pw = new PrintWriter(outStream, true);
					                pw.println(result); 
								}
								else{
									OutputStream outStream = socket.getOutputStream();             
					                PrintWriter pw = new PrintWriter(outStream, true);
					                pw.println(result);
								}
							}
							else{
								Log.d(TAG,"Empty result");
								//TODO:should i write to socket?
							}
						}
												
					}
					System.out.flush();
				} 
			} catch (IOException e) {
				// TODO Auto-generated catch block
				Log.e(TAG,"Error in server task");
				e.printStackTrace();
			}         
            catch(Exception e){
            	Log.e(TAG,"Generic Exception in server task");
            	e.printStackTrace();
            }
            return null;
        }

        public String routeChord(String key,String value,String messageType,String initiator,String setSuccessor,String setPredecessor,String queryResult)
        {
        	Log.d(TAG,"Inside routechord:message type="+messageType);
        	String newSuccesor="";
        	String newPredecessor="";
        	String destinationPort="";// - who to forward the message to
        	//initiator -original sender of request(join/insert/query/delete)
        	try{
        		
    	    	if(messageType.equals("INSERT_REQUEST"))
    	    	{
    	    		Log.d(TAG,"inside insert request at port="+portStr);
    	    		//Log.d(TAG,"hash of key is="+genHash(key));
    	    		//check if key is already present or not and then add, if present, update
    	    			String q = "select * from Dynamo where key=?";
    	    			String selectionArgs[]=new String[1];
    	    			selectionArgs[0]=key;
    	    			Cursor mCursor = db.rawQuery(q, selectionArgs);
    	    			
    	    			if(mCursor.getCount()>0){
    	    				//key already present,update
    	    				Log.d(TAG,"Key already present at the time of insert");
    	    				ContentValues cv = new ContentValues();
	        	    		//new value
    	    				
	        	    		cv.put(VALUE_FIELD,value);
	        	    		String whereClause="key=?";
	        	    		
	        	    		db.update("Dynamo",cv,whereClause,new String[]{key});
    	    			}
    	    			else{
	        	    		ContentValues cv = new ContentValues();
	        	    		Log.d(TAG,"Inserting first time");
	        	        	//hash key to find position in ring to store
	        	    		cv.put(KEY_FIELD, key);
	        	    		cv.put(VALUE_FIELD,value);
	        	    		db.insert("Dynamo", null, cv);
	        	    	    Log.d("inserted key="+key+" at node="+portStr, cv.toString());
    	    			}
    	    	}	
    	    	if(messageType.equals("QUERY_REQUEST"))
    	    	{
    	    		Log.d(TAG,"inside query request at "+portStr);
    	    		
    	    			Cursor resultCursor = db.query("Dynamo", null,"key='"+key+"'", null, null, null, null);
    	    			Log.d(TAG,"Query particular key= "+key+" : at "+portStr+" Cursor size="+resultCursor.getCount());
    	    			if(resultCursor.getCount()>0)
    	    			{
    	    				resultCursor.moveToFirst();
    	    				int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
    	    				int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
    	    				String returnKey = resultCursor.getString(0);
    	    				String returnValue = resultCursor.getString(1);
    	    				Log.d(TAG,"inside particular query request "+returnKey+" value="+returnValue);
    	    				resultCursor.close();
    	    				//add contents of cursor in arraylist and send it back to initiator
    	    				ArrayList list = new ArrayList();
    	    				list.add(returnKey);
    	    				list.add(returnValue);
    	    				Log.d(TAG,"array list contents="+list.toArray());
    	    				String dest=convertToPort(initiator);
    	    				Log.d(TAG,"initiator is="+initiator);
    	    				Log.d(TAG,"send to initiator="+dest);
    	    				//return query result,in route chord,write to socket
    	    				return TextUtils.join("::", list);
    	    				   	    				
    	    			}
    	    			else
    	    				return "";    				
    	    	}
    	
    	    	if(messageType.equals("GLOBAL_DUMP"))
    	    	{
    	    		Log.d(TAG,"inside global dump at "+portStr);
    	    		Cursor resultCursor = db.query("Dynamo", null,null, null, null, null, null);
    	    		int i=0;
    	    		ArrayList list = new ArrayList();
    	    		
    	    		if(resultCursor.getCount()>0)
    	    		{
    	    			resultCursor.moveToFirst();
    	    			while(i<resultCursor.getCount())
    		    		{
    		    			int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
    						int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
    						Log.d(TAG,"key index="+keyIndex+" valueIndex="+valueIndex);
    						String returnKey = resultCursor.getString(0);
    						String returnValue = resultCursor.getString(1);
    						//add contents of cursor in arraylist
    						list.add(returnKey);
    						list.add(returnValue);
    						resultCursor.moveToNext();
    						i++;
    		    		}
    	    			return TextUtils.join("::", list);
    	    		}
    	    		else{
    	    			return "";
    	    		}
    	    		
    	    	}
    	    	if(messageType.equals("DELETE_REQUEST"))
    	    	{
    	    		Log.d(TAG,"inside particular key delete request at "+portStr);
    	    		deletedRows=db.delete("Dynamo", "key='"+key+"'", null);
    				
    	    	}
    	    	if(messageType.equals("GLOBAL_DELETE"))
    	    	{
    	    		Log.d(TAG,"inside global delete");
    	    		int no=db.delete("Dynamo", null, null);
    	    	}
    	    	if(messageType.equals("RECOVERY_REQUEST"))
    	    	{
    	    		Log.d(TAG,"inside recovery request at "+portStr);
    	    		//check if requestor is successor or predecessor
    	    		//do an @ query
    	    		//if successor, then filter using my node id
    	    		//if predecessor, then filter using requestor's node id
    	    		Cursor resultCursor = db.query("Dynamo", null,null, null, null, null, null);
	    			Log.d(TAG,"Query local dump at "+portStr+" Cursor size="+resultCursor.getCount());
	    			int i=0;
	    			ArrayList list = new ArrayList();
	    			if(resultCursor.getCount()>0)
	    			{
	    				resultCursor.moveToFirst();
    	    			while(i<resultCursor.getCount())
    		    		{
    		    			int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
    						int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
    						Log.d(TAG,"key index="+keyIndex+" valueIndex="+valueIndex);
    						String returnKey = resultCursor.getString(0);
    						String returnValue = resultCursor.getString(1);
    						if(initiator.equals(successor1) || initiator.equals(successor2))
    						{
    							Log.d(TAG,"filter using my id");
	    						if(containsKey(returnKey,portStr))
	    						{
	    							Log.d(TAG,"Key should be added");
		    						list.add(returnKey);
		    						list.add(returnValue);
	    						}
    						}
    						else if(initiator.equals(predecessor1) || initiator.equals(predecessor2))
    						{
    							Log.d(TAG,"filter using initiator");
    							if(containsKey(returnKey,initiator))
	    						{
	    							Log.d(TAG,"Key should be added");
		    						list.add(returnKey);
		    						list.add(returnValue);
	    						}
    						}
    						resultCursor.moveToNext();
    						i++;
    		    		}
    	    			return TextUtils.join("::", list);
	    			}
	    			else{
	    				Log.d(TAG,"Cursor is empty");
	    				return "";
	    			}
    	    	}
    	    	
        	} catch (Exception e) {
    		// TODO Auto-generated catch block
        		Log.e(TAG,"error in route chord");
        		e.printStackTrace();
        	}
        	return null;
        }
    }
    
    public class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
        	//order-message type,key, value,initiator,destination,successor,predecessor,query
        	String initiator="";//original sender of message
        	String key="";
        	String value="";
        	String successor="";
        	String predecessor="";
        	String destination="";
        	String queryResult="";
        	String messageType="";
            try {
            	destination= msgs[4];
            	Log.d(TAG,"Destination in client task="+destination);
            	Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                Integer.parseInt(destination));
            	messageType=msgs[0];
            	key=msgs[1];
            	value=msgs[2];
            	initiator=msgs[3];
            	successor=msgs[5];
            	predecessor=msgs[6];
            	queryResult=msgs[7];
               // System.out.println("Msg is-->"+message);
                OutputStream outStream = socket.getOutputStream();             
                PrintWriter pw = new PrintWriter(outStream, true);
                Log.d(TAG,"Client message="+messageType+"!"+key+"@"+value+"#"+initiator+"$"+successor+"%"+predecessor+"^"+queryResult);
                pw.println(messageType+"!"+key+"@"+value+"#"+initiator+"$"+successor+"%"+predecessor+"^"+queryResult);
                Log.d(TAG,"Wrote to socket in client task at "+portStr);
                System.out.flush();
                socket.close();
               
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            catch(Exception e){
            	Log.e(TAG,"Exception in Client task");
            	e.printStackTrace();
            }
           return null; 
        }
    }
    public void getMyPort(){
    	TelephonyManager tel = (TelephonyManager)getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        System.out.println("AVD is "+myPort);
    }
    
    public String convertToPort(String str){
    	//Log.d(TAG,"inside convertToPort,value is="+str);
    	return String.valueOf((Integer.parseInt(str) * 2));
    }
    
    public String connectServer(String messageType,String key,String value,String initiator, String destination, String successor, String predecessor, String query ){
    	Log.d(TAG,"Message type :  (query from) "+destination);;
    	String destPort="";
        try {
        	destPort= convertToPort(destination);
        	Log.d(TAG,"destination is="+destPort);
        	Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
            Integer.parseInt(destPort));
        	
            OutputStream outStream = socket.getOutputStream();             
            PrintWriter pw = new PrintWriter(outStream, true);
            Log.d(TAG,"Client message to be sent directly ="+messageType+"!"+key+"@"+value+"#"+initiator+"$"+successor+"%"+predecessor+"^"+query);
            pw.println(messageType+"!"+key+"@"+value+"#"+initiator+"$"+successor+"%"+predecessor+"^"+query);
            System.out.flush();
            
            //continue reading on the same socket
            InputStream inStream = socket.getInputStream(); 
			BufferedReader fromClient = new BufferedReader(new InputStreamReader(inStream)); 
			String s=fromClient.readLine();
			//received result of local dump from one avd
			Log.d(TAG,"Received result of "+destination+"="+s);
			return s;
			//TODO:time out will be triggered here,create socket in catch to send to the successor
			//close everything
			//pw.close();
			//fromClient.close();
            //socket.close();
        } catch (UnknownHostException e) {
            Log.e(TAG, "ClientTask UnknownHostException");
        } catch (IOException e) {
            Log.e(TAG, "ClientTask socket IOException");
        }	
        catch(Exception e){
        	Log.e(TAG,"Socket Timeout Exception in connect server");
        	e.printStackTrace();
        		//open socket and send query to successor
        	
			try {
				//new destination is old destination's successor
				int i=0;
				for(i=0;i<nodeList.length;i++){
					if(destination.equals(nodeList[i])){
						break;
					}
				}
				//for 3 avds
				//String newDest=	nodeList[(i+1)%3];
				//Log.d(TAG,"new destination is "+newDest);
				//for 5 avds
				String newDest=	nodeList[(i+1)%5];
				Log.d(TAG,"new destination is "+newDest);
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
				        Integer.parseInt(convertToPort(newDest)));
			 //send to the node's successor not mine
                	
                    OutputStream outStream = socket.getOutputStream();             
                    PrintWriter pw = new PrintWriter(outStream, true);
                    Log.d(TAG,"Client message to be sent directly ="+messageType+"!"+key+"@"+value+"#"+initiator+"$"+successor+"%"+predecessor+"^"+query);
                    pw.println(messageType+"!"+key+"@"+value+"#"+initiator+"$"+successor+"%"+predecessor+"^"+query);
                    System.out.flush();
                    
                    //continue reading on the same socket
                    InputStream inStream = socket.getInputStream(); 
        			BufferedReader fromClient = new BufferedReader(new InputStreamReader(inStream)); 
        			String s=fromClient.readLine();
        			//received result of local dump from one avd
        			Log.d(TAG,"Received result of "+newDest+"="+s);
        			return s;// TODO: check if s is non empty
        			/*// TODO:check if null
        			if(s.equals(null)){
						Log.d(TAG,"Response is null");
						continue;
					}*/
        			
			} catch (NumberFormatException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (UnknownHostException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}catch(Exception e2){
            	Log.e(TAG,"Exception in exception catch of connect server");
            	e.printStackTrace();
            }
        }
        return null;
    }
    
    public void getNetworkInfo(){
    	int i=0;
    	for(i=0;i<nodeList.length;i++){
    		if(portStr.equals(nodeList[i])){
    				break;
    		}
    	}
    	//for 3 avds
//    	predecessor1=nodeList[(i+1)%3];;
//    	predecessor2=nodeList[(i+2)%3];
//    	successor1=nodeList[(i+1)%3];
//    	successor2=nodeList[(i+2)%3];
//    	Log.d(TAG,"My id="+portStr+", successor1="+successor1+", successor2="+successor2+", predecessor1="+predecessor1+", predecessor2="+predecessor2);
    	//for 5 avds
    	predecessor1=nodeList[(i+3)%5];
    	predecessor2=nodeList[(i+4)%5];
    	successor1=nodeList[(i+1)%5];
    	successor2=nodeList[(i+2)%5];
    	Log.d(TAG,"My id="+portStr+", successor1="+successor1+", successor2="+successor2+", predecessor1="+predecessor1+", predecessor2="+predecessor2);
    }
    public boolean containsKey(String key,String node)
    {
    	Log.d(TAG,"Inside containsKey method at "+portStr);
		try {
			if((genHash(predecessor2).compareTo(genHash(node))>0) && ((genHash(key).compareTo(genHash(node))<=0) ||(genHash(key).compareTo(genHash(predecessor2))>0)))
				return true;
			else if((genHash(key).compareTo(genHash(predecessor2))>0 && (genHash(key).compareTo(genHash(node))<=0)))
					return true;
			else
				return false;
		
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			Log.e(TAG,"No such algorithm exception in containsKey");
			e.printStackTrace();
			return false;
		}
    }
    public class RecoveryTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
        	String initiator="";//original sender of message
        	String key="";
        	String value="";
        	String successor="";
        	String predecessor="";
        	String destination="";
        	String queryResult="";
        	String messageType="";
            try {
            	destination= msgs[4];
            	Log.d(TAG,"Destination in recovery task="+destination);
            	Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                Integer.parseInt(destination));
            	messageType=msgs[0];
            	key=msgs[1];
            	value=msgs[2];
            	initiator=msgs[3];
            	successor=msgs[5];
            	predecessor=msgs[6];
            	queryResult=msgs[7];
            	
                OutputStream outStream = socket.getOutputStream();             
                PrintWriter pw = new PrintWriter(outStream, true);
                Log.d(TAG,"Recovery message to be sent directly ="+messageType+"!"+key+"@"+value+"#"+initiator+"$"+successor+"%"+predecessor+"^"+queryResult);
                pw.println(messageType+"!"+key+"@"+value+"#"+initiator+"$"+successor+"%"+predecessor+"^"+queryResult);
                Log.d(TAG,"Wrote to socket in recovery task at "+portStr);
                System.out.flush();
                
                //continue reading on the same socket
                InputStream inStream = socket.getInputStream(); 
    			BufferedReader fromClient = new BufferedReader(new InputStreamReader(inStream)); 
    			String s=fromClient.readLine();
    			//received result of local dump from one avd
    			Log.d(TAG,"Received result of "+destination+"="+s);
    			//insert results into my avd after checking for duplicates
    			Log.d(TAG,"Inserting keys on recovery at port="+portStr);
    	    	//check if key is already present or not and then add, if present, update
    	    			
    			if(s!=null && !s.isEmpty()){
    	    		String keyValues[]=s.split("::");
    	    		Log.d(TAG," keyvalues array="+keyValues.length);
    	    		for(int j=0;j<keyValues.length;j++)
    	    			Log.d(TAG,"Query result array[="+j+"]"+keyValues[j]);
    	    		int i=0;
    	    		while(i<keyValues.length)
    	    		{
    	    			String temp[]=new String[2];
    	    			temp[0]=keyValues[i++]; //key
    	    			temp[1]=keyValues[i]; //value
    	    			Log.d(TAG,"key value= "+temp[0]+" "+temp[1]);
    	    			String q = "select * from Dynamo where key=?";
    	    			String selectionArgs[]=new String[1];
    	    			selectionArgs[0]=temp[0];
    	    			Cursor mCursor = db.rawQuery(q, selectionArgs);
    	    			
    	    			if(mCursor.getCount()>0){
    	    				//key already present,update
    	    				Log.d(TAG,"Key already present at the time of insert");
    	    				ContentValues cv = new ContentValues();
	        	    		//new value
    	    				
	        	    		cv.put(VALUE_FIELD,temp[1]);
	        	    		String whereClause="key=?";
	        	    		//if(i%2!=0)
	        	    		db.update("Dynamo",cv,whereClause,new String[]{temp[0]});
    	    			}
    	    			else{
	        	    		ContentValues cv = new ContentValues();
	        	    		Log.d(TAG,"Inserting first time");
	        	        	//hash key to find position in ring to store
	        	    		cv.put(KEY_FIELD, temp[0]);
	        	    		cv.put(VALUE_FIELD,temp[1]);
	        	    		if(i%2!=0)
	        	    		db.insert("Dynamo", null, cv);
	        	    	    Log.d("inserted key="+temp[0]+" at node="+portStr, cv.toString());
    	    			}
    	    			i++;
    	    		}
    	    		
        		}
    			else{
	    			Log.d(TAG,"No key inserted on recovery");
	    		}
	    		
    			//TODO: Should i handle timeout exception here?
    			//TODO:time out will be triggered here,create socket in catch to send to the successor
    			//close everything
    			//pw.close();
    			//fromClient.close();
                //socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            catch(Exception e){
            	Log.e(TAG,"Exception in Recovery task");
            	e.printStackTrace();
            }
           return null; 
        }
    }
}
