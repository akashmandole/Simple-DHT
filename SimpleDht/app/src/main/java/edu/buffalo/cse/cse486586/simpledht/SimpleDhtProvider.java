package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();

    static final String database_name = "dht_db";
    static final String table_name = "dht_record";

    static final String create_table = "CREATE TABLE "+table_name+" (key TEXT, value TEXT);";

    static final int SERVER_PORT = 10000;
    String myPort;

    // Checking orignal query port
    static String orignalQueryPort = null;

    // lock
    static boolean lock = false;
    public static HashMap<String, String> cursorList = new HashMap<String, String>();

    // Consider remote port 0 as coordinator
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";

    private static class DBHelper extends SQLiteOpenHelper {
        DBHelper(Context context){
            super(context, database_name, null, 1);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL(create_table);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            db.execSQL("DROP TABLE IF EXISTS " +  table_name + ";");
            onCreate(db);
        }
    }

    private DBHelper dbHelper;

    static public void acquireLock() {
        lock = true;
    }

    static public void releaseLock() {
        lock = false;
    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        int rows = 0;
        SQLiteDatabase db_write = dbHelper.getWritableDatabase();

        if (selection.equals("@")) {
            rows = db_write.delete(table_name, null, null);
            Log.v("delete completed for : ", selection);

        } else if (selection.equals("*")) {

            if (myPort.equals(ServerTask.successor)) {
                rows = db_write.delete(table_name, null, null);
                return rows;
            }

            else {

                rows = db_write.delete(table_name, null, null);

                Message m = new Message();
                m.setType("delete_global");
                m.setQuery(selection);
                new ClientTask(m).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, ServerTask.successor);
                return rows;
            }

        } else {

            String myHash = ServerTask.myHash;
            String predecessorHash = ServerTask.predecessorHash;
            try {
                String key = genHash(selection);

                boolean case1 = myHash.equals(predecessorHash);
                boolean case2 = (compare(myHash,key) && compare(key,predecessorHash));
                boolean case3 = (compare(predecessorHash,myHash) && compare(myHash,key));
                boolean case4 = (compare(predecessorHash, myHash) && compare(key,predecessorHash));

                if(case1 || case2 || case3 || case4) {

                    rows = db_write.delete(table_name, "key= ?", new String[] { selection });
                    Log.v("delete completed for : ", selection);
                    return rows;

                } else {

                    // forward message to successor

                    Message forwardToSuccessor = new Message();
                    forwardToSuccessor.setType("delete");
                    forwardToSuccessor.setQuery(selection);
                    new ClientTask(forwardToSuccessor).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, ServerTask.successor);

                    return -1;
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        return rows;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    // compare two strings
    private boolean compare(String s1, String s2) {
        if(s1.compareTo(s2)>0) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        String myHash = ServerTask.myHash;
        String predecessorHash = ServerTask.predecessorHash;

        try {

            String key = SimpleDhtProvider.genHash((String) values.get("key"));

            boolean case1 = myHash.equals(predecessorHash);
            boolean case2 = (compare(myHash,key) && compare(key,predecessorHash));
            boolean case3 = (compare(predecessorHash,myHash) && compare(myHash,key));
            boolean case4 = (compare(predecessorHash,myHash) && compare(key,predecessorHash));

            if(case1 || case2 || case3 || case4) {

                SQLiteDatabase db_write = dbHelper.getWritableDatabase();
                long id = db_write.insertWithOnConflict(table_name, null, values, SQLiteDatabase.CONFLICT_REPLACE);

                if (id > 0) {
                    Log.v("inserting locally : ", values.toString());
                    return Uri.withAppendedPath(uri, Long.toString(id));
                }

            } else {

                // forward message to successor

                Message forwardToSuccessor = new Message("insert",(String) values.get("key"),(String) values.get("value"));
                new ClientTask(forwardToSuccessor).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, ServerTask.successor);
                return null;
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean onCreate() {

        dbHelper = new DBHelper(getContext());

        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            String myHash = genPortHash(myPort);
            new ServerTask(getContext(), myPort, myHash).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if (Integer.parseInt(myPort) != Integer.parseInt(REMOTE_PORT0)) {
            try {
                Message message = new Message(myPort,"join", null, null);
                Log.v("Initiated join : ", myPort);
                new ClientTask(message).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, REMOTE_PORT0);

            } catch (Exception e) {
                Log.e(TAG, "Coordinator port not found");
            }
        }

        return true;
    }

    public class ClientTask extends AsyncTask<String, Void, Void> {

        Message message;

        public ClientTask(Message message) {
            this.message = message;
        }

        @Override
        protected Void doInBackground(String... msgs) {
            try {

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[0]));

                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeObject(message);
                objectOutputStream.flush();
                objectOutputStream.close();
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {

        SQLiteDatabase db_read = dbHelper.getReadableDatabase();

        String myHash = ServerTask.myHash;
        String predecessorHash = ServerTask.predecessorHash;

        if(selection.equals("@")) {
            Cursor cursor = db_read.rawQuery("Select * from " + table_name, selectionArgs);
            Log.v("query completed for : ", selection);
            return cursor;
        }

        Cursor cursor;

        if (selection.equals("*")) {


            if(myHash.equals(predecessorHash)) {
                cursor = db_read.rawQuery("Select * from " + table_name, selectionArgs);
                Log.v("query completed for : ", selection);
                return cursor;

            } else {

                if (orignalQueryPort == null) {
                    orignalQueryPort = myPort;
                    acquireLock();
                }

                cursor = db_read.rawQuery("Select * from " + table_name, selectionArgs);
                cursor.moveToFirst();
                for (int i = 0 ; i<cursor.getCount();i++) {
                    cursorList.put(cursor.getString(cursor.getColumnIndex("key")), cursor.getString(cursor.getColumnIndex("value")));
                    cursor.moveToNext();
                }
                cursor.close();

                // forward to successor

                Message forwardToSuccessor = new Message(orignalQueryPort, "query_global", null, null, selection, cursorList);
                new ClientTask(forwardToSuccessor).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, ServerTask.successor);
                ServerTask.matrixCursor = null;

                while (lock) {
                    // wait for some time
                }
                releaseLock();
                orignalQueryPort = null;
                return ServerTask.matrixCursor;
            }
        } else {

            try {

                String key = genHash(selection);

                boolean case1 = myHash.equals(predecessorHash);
                boolean case2 = (compare(myHash,key) && compare(key,predecessorHash));
                boolean case3 = (compare(predecessorHash,myHash) && compare(myHash,key));
                boolean case4 = (compare(predecessorHash,myHash) && compare(key,predecessorHash));

                if(case1 || case2 || case3 || case4) {

                    cursor = db_read.query(table_name, projection, "key=?", new String[]{selection}, null, null, sortOrder);
                    Log.v("query completed for : ", selection);
                    return cursor;

                } else {

                    if (orignalQueryPort == null) {
                        orignalQueryPort = myPort;
                        acquireLock();
                    }

                    // forward to successor

                    Message forwardToSuccessor = new Message();
                    forwardToSuccessor.setType("query");
                    forwardToSuccessor.setMyPort(orignalQueryPort);
                    forwardToSuccessor.setQuery(selection);

                    new ClientTask(forwardToSuccessor).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, ServerTask.successor);

                    while (lock) {
                        // wait for some time..
                    }
                    releaseLock();
                    orignalQueryPort = null;

                    return ServerTask.matrixCursor;
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public static class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        static final String TAG = SimpleDhtProvider.class.getSimpleName();

        public static String myPort;
        public static String myHash;
        public static String successor;
        public static String successorHash;
        public static String predecessor;
        public static String predecessorHash;
        private Context context;

        static final String url = "content://edu.buffalo.cse.cse486586.simpledht.provider";
        static final Uri simple_dht_uri = Uri.parse(url);

        public static Cursor matrixCursor = null;


        public ServerTask(Context context, String myPort, String myHash) {

            this.myPort = myPort;
            this.myHash = myHash;
            this.predecessor = myPort;
            this.predecessorHash = myHash;
            this.successor = myPort;
            this.successorHash = myHash;
            this.context = context;
        }

        // compare to strings
        private boolean compare(String s1, String s2) {
            if(s1.compareTo(s2)>0) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            while(true) {
                try {

                    Socket serverListener = serverSocket.accept();
                    ObjectInputStream objectInputStream = new ObjectInputStream(serverListener.getInputStream());
                    Message message = (Message) objectInputStream.readObject();

                    if(message.type.equals("join")) {

                        Log.v("join received for : ", message.getMyPort());

                        String client = message.getMyPort();
                        String clientHash = SimpleDhtProvider.genPortHash(client);

                        boolean case1 = myHash.equals(predecessorHash);
                        boolean case2 = (compare(myHash,clientHash) && compare(clientHash,predecessorHash));
                        boolean case3 = (compare(predecessorHash,myHash) && compare(myHash,clientHash));
                        boolean case4 = (compare(predecessorHash,myHash) && compare(clientHash,predecessorHash));

                        if(case1 || case2 || case3 || case4) {

                            Message updateClient = new Message(client, "accept", myPort, predecessor);
                            forwardRequest(updateClient, client);

                            Message updatePredecessor = new Message(client,"accept",client, null);
                            forwardRequest(updatePredecessor, predecessor);

                            predecessor = client;
                            predecessorHash = clientHash;

                        } else {

                            // Forward join request to successor
                            Message forwardToSuccessor = new Message(client,"join",null,null);
                            forwardRequest(forwardToSuccessor,successor);

                        }
                    } else if (message.type.equals("accept")) {

                        Log.v("accept received for : ", message.getMyPort());

                        String newPredecessor = message.getPredecessor();
                        if (newPredecessor != null) {
                            predecessor = newPredecessor;
                            predecessorHash = SimpleDhtProvider.genPortHash(predecessor);
                        }
                        String newSuccessor = message.getSuccessor();
                        if (newSuccessor != null) {
                            successor = newSuccessor;
                            successorHash = SimpleDhtProvider.genPortHash(successor);
                        }

                    } else if (message.type.equals("insert")) {

                        ContentValues keyValueToInsert = new ContentValues();

                        keyValueToInsert.put("key", message.getKey());
                        keyValueToInsert.put("value", message.getValue());

                        Log.v("Key to insert : ", message.getKey());

                        Uri uri = context.getContentResolver().insert(simple_dht_uri, keyValueToInsert);

                    } else if (message.type.equals("query")) {

                        matrixCursor = null;
                        SimpleDhtProvider.orignalQueryPort = message.getMyPort();
                        SimpleDhtProvider.releaseLock();

                        Cursor cursor = context.getContentResolver().query(simple_dht_uri, null, message.getQuery(), null, null);

                        if (cursor != null) {

                            Log.v("query response send : ", message.getMyPort());

                            Message msg = new Message();
                            msg.setType("query_response");
                            msg.setMyPort(message.getMyPort());
                            msg.setQuery(message.getQuery());

                            cursor.moveToFirst();

                            msg.setKey(cursor.getString(cursor.getColumnIndex("key")));
                            msg.setValue(cursor.getString(cursor.getColumnIndex("value")));
                            cursor.close();

                            forwardRequest(msg,message.getMyPort());
                        }

                        SimpleDhtProvider.releaseLock();
                        SimpleDhtProvider.orignalQueryPort = null;

                    } else if (message.type.equals("query_response")) {

                        matrixCursor = new MatrixCursor(new String[] {"key", "value" });

                        Log.v("query response reciv : ", message.getMyPort());

                        ((MatrixCursor) matrixCursor).addRow(new String[] { message.getKey(), message.getValue() });
                        SimpleDhtProvider.releaseLock();
                    } else if (message.type.equals("query_global")) {

                        matrixCursor = null;
                        SimpleDhtProvider.orignalQueryPort = message.getMyPort();
                        SimpleDhtProvider.releaseLock();

                        if(SimpleDhtProvider.orignalQueryPort.equals(myPort)) {
                            matrixCursor = new MatrixCursor(new String[] {"key", "value"});

                            Iterator iterator = message.getCursorList().entrySet().iterator();

                            while (iterator.hasNext()) {

                                Map.Entry entry = (Map.Entry)iterator.next();
                                ((MatrixCursor) matrixCursor).addRow(new String[] {(String)entry.getKey(), (String)entry.getValue() });

                            }

                            SimpleDhtProvider.releaseLock();

                        } else {
                            SimpleDhtProvider.cursorList = (HashMap<String, String>) message.getCursorList();
                            context.getContentResolver().query(simple_dht_uri, null, message.getQuery(), null, null);

                        }

                        SimpleDhtProvider.releaseLock();
                        SimpleDhtProvider.orignalQueryPort = null;

                    } else if (message.type.equals("delete")) {

                        int rows = context.getContentResolver().delete(simple_dht_uri, message.getQuery(), null);

                    } else if (message.type.equals("delete_global")) {

                        int rows = context.getContentResolver().delete(simple_dht_uri, message.getQuery(), null);
                    }  else  {
                        Log.v("Invalid Message Type : ", message.type);
                    }

                    objectInputStream.close();
                    serverListener.close();

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }

        }

        private void forwardRequest(Message message,String port) {
            try {

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));

                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeObject(message);
                objectOutputStream.flush();
                objectOutputStream.close();
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "Unable to forward request");
            } catch (IOException e) {
                Log.e(TAG, "Unable to forward request");
            }
        }
    }


    public static String genPortHash(String port) throws NoSuchAlgorithmException {
        return genHash(String.valueOf(Integer.parseInt(port)/2));
    }

    public static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

}
