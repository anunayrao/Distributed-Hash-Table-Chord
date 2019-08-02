package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.Buffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    static String myavdPort="";    //5554 Format
    String predecessor="";         //5554 Format
    String successor="";            //5554 Format
    ArrayList<String> remotePort = new ArrayList<String>();
    ArrayList<Node> ring = new ArrayList<Node>();
    ArrayList<String> Files = new ArrayList<String>();
    BlockingQueue<String> AllKeyVal = new ArrayBlockingQueue<String>(1);
    BlockingQueue<String> QueryKeyVal = new ArrayBlockingQueue<String>(1);
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if(selection.contains("@")){
            //https://stackoverflow.com/questions/3554722/how-to-delete-internal-storage-file-in-android
            File dir[] = getContext().getFilesDir().listFiles();
            for( File file : dir){
                file.delete();
            }
        }
        else if(selection.contains("*")){
            File dir[] = getContext().getFilesDir().listFiles();
            for( File file : dir){
                file.delete();
            }
            String msg= "Deleteall";
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg,successor);
            Log.d(TAG, "Deleted all Successfully");
        }
        else{

            File dir = getContext().getFilesDir();
            File file = new File(dir, selection);
            if(file.exists()){
                file.delete();
            }

        }
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {


        String key = values.getAsString("key");
        String value = values.getAsString("value");
        String hashkey=null;
        String myhash =null;
        String phash = null;
        String shash = null;
        Log.d(TAG, "Insert Called for:"+key);
        Log.d(TAG, "Successor:"+successor);
        Log.d(TAG, "Predecessor:"+predecessor);
        try {
            hashkey = genHash(key);
            myhash = genHash(myavdPort);
            phash = genHash(predecessor);
            shash = genHash(successor);

        //if((predecessor.equals("") || successor.equals("")|| (hashkey.compareTo(phash)>0 && hashkey.compareTo(myhash)<0) || (phash.compareTo(myhash)>0 && (hashkey.compareTo(phash))>0 || hashkey.compareTo(myhash)<0))) {
       // if(successor==""|| (hashkey.compareTo(myhash)>0 && hashkey.compareTo(phash)>0 && myhash.compareTo(phash)<0)||(hashkey.compareTo(myhash)<0 && hashkey.compareTo(phash)>0 && myhash.compareTo(phash)>0) || (hashkey.compareTo(myhash)<=0 && myhash.compareTo(phash)<0) ){
        // if(successor==""|| myhash.compareTo(phash)>0 && hashkey.compareTo(myhash)<=0 && hashkey.compareTo(phash)>0 || myhash.compareTo(phash)<0 && (hashkey.compareTo(phash)>0 || hashkey.compareTo(myhash)<=0)){
        //if(predecessor==""||(myhash.compareTo(phash)<0 && (hashkey.compareTo(myhash)<=0 || hashkey.compareTo(phash)>0)) || (phash.compareTo(myhash)<0 && (hashkey.compareTo(myhash)<=0 && hashkey.compareTo(phash)>0)) )  {
       /* if (predecessor == "" ||( successor==predecessor && successor!="" && hashkey.compareTo(myhash)>0 && hashkey.compareTo(shash)>0 ) || (hashkey.compareTo(myhash) >= 0 && myhash.compareTo(phash) < 0 && hashkey.compareTo(phash) > 0) ||(hashkey.compareTo(myhash)<=0 && hashkey.compareTo(phash)<0 && hashkey.compareTo(shash)<0 && myhash.compareTo(phash)<0) || (hashkey.compareTo(myhash)<=0 && hashkey.compareTo(phash)>0) || (hashkey.compareTo(myhash)>=0 && hashkey.compareTo(phash)>0 && myhash.compareTo(phash)<0)) {
            //Refrenece: https://stackoverflow.com/questions/14376807/how-to-read-write-string-from-a-file-in-android
            Files.add(key);
            Log.d(TAG,"Inserted:::::"+key);

            Log.d(TAG, "FILE SIZE:::"+ Files.size()+":at:"+myavdPort);
        try {
            OutputStreamWriter osw = new OutputStreamWriter(getContext().openFileOutput(key, Context.MODE_PRIVATE));
            osw.write(value);
            osw.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        }*/
       if(predecessor=="" || successor ==""){
           Files.add(key);
           Log.d(TAG,"Inserted:::::"+key);

           Log.d(TAG, "FILE SIZE:::"+ Files.size()+":at:"+myavdPort);
           try {
               OutputStreamWriter osw = new OutputStreamWriter(getContext().openFileOutput(key, Context.MODE_PRIVATE));
               osw.write(value);
               osw.close();
           } catch (FileNotFoundException e) {
               e.printStackTrace();
           } catch (IOException e) {
               e.printStackTrace();
           }
       }
       else if((genHash((key))).compareTo(genHash(myavdPort))>0 && genHash(key).compareTo(genHash(predecessor))>0 && genHash(myavdPort).compareTo(genHash(predecessor))<0){

               Files.add(key);
               Log.d(TAG,"Inserted:::::"+key);

               Log.d(TAG, "FILE SIZE:::"+ Files.size()+":at:"+myavdPort);
               try {
                   OutputStreamWriter osw = new OutputStreamWriter(getContext().openFileOutput(key, Context.MODE_PRIVATE));
                   osw.write(value);
                   osw.close();
               } catch (FileNotFoundException e) {
                   e.printStackTrace();
               } catch (IOException e) {
                   e.printStackTrace();
               }



       }
       else if(genHash(key).compareTo(genHash(myavdPort))<0 && genHash(key).compareTo(genHash(predecessor))<0  && genHash(predecessor).compareTo(genHash(myavdPort))>0){
           Files.add(key);
           Log.d(TAG,"Inserted:::::"+key);

           Log.d(TAG, "FILE SIZE:::"+ Files.size()+":at:"+myavdPort);
           try {
               OutputStreamWriter osw = new OutputStreamWriter(getContext().openFileOutput(key, Context.MODE_PRIVATE));
               osw.write(value);
               osw.close();
           } catch (FileNotFoundException e) {
               e.printStackTrace();
           } catch (IOException e) {
               e.printStackTrace();
           }
       }
      /* else if(hashkey.compareTo(myhash)>0 && hashkey.compareTo(phash)<0 && myhash.compareTo(phash)<0 && shash.compareTo(myhash)>0){
           Files.add(key);
           Log.d(TAG,"Inserted:::::"+key);

           Log.d(TAG, "FILE SIZE:::"+ Files.size()+":at:"+myavdPort);
           try {
               OutputStreamWriter osw = new OutputStreamWriter(getContext().openFileOutput(key, Context.MODE_PRIVATE));
               osw.write(value);
               osw.close();
           } catch (FileNotFoundException e) {
               e.printStackTrace();
           } catch (IOException e) {
               e.printStackTrace();
           }

       }*/
       else if(genHash(key).compareTo(genHash(myavdPort))<0 && genHash(key).compareTo(genHash(predecessor))>0 && genHash(myavdPort).compareTo(genHash(predecessor))>0){
           Files.add(key);
           Log.d(TAG,"Inserted:::::"+key);

           Log.d(TAG, "FILE SIZE:::"+ Files.size()+":at:"+myavdPort);
           try {
               OutputStreamWriter osw = new OutputStreamWriter(getContext().openFileOutput(key, Context.MODE_PRIVATE));
               osw.write(value);
               osw.close();
           } catch (FileNotFoundException e) {
               e.printStackTrace();
           } catch (IOException e) {
               e.printStackTrace();
           }

       }

        //else if((myhash.compareTo(phash)<0 && (hashkey.compareTo(myhash)>0 || (hashkey.compareTo(myhash)<0 && hashkey.compareTo(phash)<0))) || (phash.compareTo(myhash)<0 && (hashkey.compareTo(myhash)>0 || (hashkey.compareTo(myhash)<0 && hashkey.compareTo(phash)<0))))
        else  {
            Log.d(TAG, "Calling successor to insert:"+key);

            String str = "Insert:"+key+":"+value;
            String successorPort = String.valueOf(Integer.parseInt(successor)*2);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,str, successorPort);

            Log.d(TAG, "Client Called to pass its key as predecessor to next:");
        }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


            // TODO Auto-generated method stub
            return null;

    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        remotePort.add(REMOTE_PORT0);
        remotePort.add(REMOTE_PORT1);
        remotePort.add(REMOTE_PORT2);
        remotePort.add(REMOTE_PORT3);
        remotePort.add(REMOTE_PORT4);

        String myPortID = getMyPort();
        //11108:5554
        String myPort = myPortID.split(":")[0];
        String myID = myPortID.split(":")[1];
        String port = myID;   //5554 Format
        myavdPort = myID;  //5554 Format


        try {
            ServerSocket  serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't Create Server Socket");
            e.printStackTrace();
        }


        if(myPort.equals(REMOTE_PORT0)){
            try {
                Log.d(TAG,"Port NO:"+myPort+"Active");
                myID = genHash(myID);

            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            Node newNode = new Node(myID, null, null,port, myPort);
            ring.add(newNode);
            Log.i(TAG, "Port 5554 is Live and ready for Join Requests");
        }

        else if(remotePort.contains(myPort)){

            Log.i(TAG, "Join Request by:"+myID);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "JoinMe:"+myID+":"+myPort,REMOTE_PORT0);
        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        Log.d(TAG, "MyPort:"+myavdPort+":Predecessor:"+predecessor+":Successor:"+successor);
        MatrixCursor mc = new MatrixCursor(new String[] {"key", "value"});
        Context con = getContext();
        String data;
        Log.d(TAG, "QueryMethod: Selection:"+selection);
        if(selection.contains("@")){
            for(int i=0; i<Files.size();i++) {
                try {
                    FileInputStream fis = con.openFileInput(Files.get(i));
                    BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                    while ((data = br.readLine()) != null) {
                        mc.addRow(new String[]{Files.get(i), data});

                    }
                    fis.close();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return mc;
        }
        else if(selection.contains("*")){
            Log.d(TAG, "QueryMethod: Type *");
            for(int i=0; i<Files.size();i++) {
                try {
                    FileInputStream fis = con.openFileInput(Files.get(i));
                    BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                    while ((data = br.readLine()) != null) {
                        mc.addRow(new String[]{Files.get(i), data});

                    }
                    fis.close();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            String msg = "ALLKVPAIRS*";
            if(successor!="") {
                Log.d(TAG, "Calling Client from QueryMethod to Give all key value ");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, String.valueOf(Integer.parseInt(successor) * 2));

                // Collect Key Value Pairs from other nodes and add it to matrix cursor.

                String str = null;
                try {
                    str = AllKeyVal.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                String kvps[] = str.split("\\|");
                for (int i = 0; i < kvps.length; i++) {
                    Log.d(TAG, "KVPS:"+i+":"+kvps[i]);
                    if(kvps[i]!="") {
                        String k = kvps[i].split(":")[0];
                        String v = kvps[i].split(":")[1];
                        mc.addRow(new String[]{k, v});
                    }
                }
                AllKeyVal.clear();
            }
            return mc;

        }
        else{
            Log.d(TAG,"QueryMethod:"+ "Looking for specific key");
            String hashkey = null;
            String phash=null;
            String myhash=null;
            String shash=null;
            try {
                hashkey = genHash(selection);
                phash = genHash(predecessor);
                myhash = genHash(myavdPort);
                shash = genHash(successor);


            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            // When key found in its own Content Provider
           // if((predecessor.equals("") || successor.equals("")|| (hashkey.compareTo(phash)>0 && hashkey.compareTo(myhash)<0) || (phash.compareTo(myhash)>0 && (hashkey.compareTo(phash))>0 || hashkey.compareTo(myhash)<0))) {
           // if(successor==""|| (hashkey.compareTo(myhash)>0 && hashkey.compareTo(phash)>0 && myhash.compareTo(phash)<0)||(hashkey.compareTo(myhash)<0 && hashkey.compareTo(phash)>0 && myhash.compareTo(phash)>0) || (hashkey.compareTo(myhash)<=0 && myhash.compareTo(phash)<0) ){
            //if(predecessor==""||(myhash.compareTo(phash)<0 && (hashkey.compareTo(myhash)<=0 || hashkey.compareTo(phash)>0)) || (phash.compareTo(myhash)<0 && (hashkey.compareTo(myhash)<=0 && hashkey.compareTo(phash)>0)) )  {
            if (predecessor == "" || (hashkey.compareTo(myhash) >= 0 && myhash.compareTo(phash) < 0 && hashkey.compareTo(phash) > 0) ||(hashkey.compareTo(myhash)<=0 && hashkey.compareTo(phash)<0 && hashkey.compareTo(shash)<0 && myhash.compareTo(phash)<0) || (hashkey.compareTo(myhash)<=0 && hashkey.compareTo(phash)>0) || (hashkey.compareTo(myhash)>=0 && hashkey.compareTo(phash)>0 && myhash.compareTo(phash)<0)) {

                Log.d(TAG,"QueryMethod:"+ "Found in my Own Content Provider");
                for(int i=0; i<Files.size();i++) {
                    if (selection.equals(Files.get(i))){
                            Log.d(TAG,"Specific filenames search:"+Files.get(i)+":Selection is :"+selection);
                        try {
                            FileInputStream fis = con.openFileInput(Files.get(i));
                            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                            while ((data = br.readLine()) != null) {
                                mc.addRow(new String[]{Files.get(i), data});

                            }
                            fis.close();
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                }
                }
            return mc;
            }
            // Check for the Key in Successor's Content Provider
            //else if((myhash.compareTo(phash)<0 && (hashkey.compareTo(myhash)>0 || (hashkey.compareTo(myhash)<0 && hashkey.compareTo(phash)<0))) || (phash.compareTo(myhash)<0 && (hashkey.compareTo(myhash)>0 || (hashkey.compareTo(myhash)<0 && hashkey.compareTo(phash)<0))))
            else
            {   Log.d(TAG,"QueryMethod:"+ "Querying Successor");
                String msg = "Query:"+selection;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg,String.valueOf(Integer.parseInt(successor)*2));

                Log.d(TAG,"QueryMethod:"+ "Blocking Array Waiting");
                String str = null;
                try {
                    str = QueryKeyVal.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Log.d(TAG,"QueryMethod:"+ "Blocking Array Got Value");
                String k = str.split(":")[0];
                String v = str.split(":")[1];
                mc.addRow(new String[]{k,v});

                QueryKeyVal.clear();
                return mc;
                //Code Here to get key
            }
        }


        // TODO Auto-generated method stub
    //return mc;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
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

    public String getMyPort() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        String myportid = myPort + ":" + portStr;
        return myportid;
    }

    public ArrayList<Node> UpdatePredSucc(ArrayList<Node> ring){
        if(ring.size()>2) {
            Collections.sort(ring);
            for (int i = 0; i < ring.size() - 1; i++) {
                Node one = ring.get(i);
                Node two = ring.get(i + 1);
                one.setSuccessor(two);
                two.setPredecessor(one);

            }
            Node first = ring.get(0);
            Node last = ring.get(ring.size() - 1);
            first.setPredecessor(last);
            last.setSuccessor(first);
            for (int i = 0; i < ring.size(); i++) {
                Log.i(TAG, "RING_UPDATE:" + ring.get(i).portno);
            }

            for (int i = 0; i < ring.size(); i++) {
                Node n = ring.get(i);
                String str = "";
                //Update:Predecessor:Successor
                str = str + "Update:";
                str = str + n.getPredecessor().getPort() + ":" + n.getSuccessor().getPort();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, str, n.getPortno());
            }
        }
        else {
            for (int i = 0; i < ring.size(); i++) {
                Node n = ring.get(i);
                String str = "";
                //Update:Predecessor:Successor
                str = str + "Update:";
                str = str + n.getPredecessor().getPort() + ":" + n.getSuccessor().getPort();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, str, n.getPortno());
            }
        }
        return ring;
    }


    class Node implements Comparable<Node> {
        String node_id;
        Node predecessor;
        Node successor;
        String port;
        String portno;

        public String getNode_id() {
            return node_id;
        }

        public String getPort() {
            return port;
        }

        public Node getPredecessor() {
            return predecessor;
        }

        public void setPredecessor(Node predecessor) {
            this.predecessor = predecessor;
        }

        public Node getSuccessor() {
            return successor;
        }

        public void setSuccessor(Node successor) {
            this.successor = successor;
        }

        public String getPortno() {
            return portno;
        }

        public Node(String node_id, Node predecessor, Node successor, String port, String portno) {
            this.node_id = node_id;
            this.predecessor = predecessor;
            this.successor = successor;
            this.port = port;
            this.portno = portno;
        }


        public int compareTo(Node o) {
            return this.node_id.compareTo(o.node_id);
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {

            ServerSocket serverSocket = serverSockets[0];
            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    InputStreamReader isr = new InputStreamReader(socket.getInputStream());
                    BufferedReader br = new BufferedReader(isr);
                    String msg = br.readLine();

                    if (msg.contains("JoinMe")) {
                        Log.d(TAG,"Join Received");
                        String NodeIDtoJoin = msg.split(":")[1];
                        String NodePorttoJoin = msg.split(":")[2];
                        String NodePort = NodeIDtoJoin;
                        NodeIDtoJoin = genHash(NodeIDtoJoin);
                        Node f =null;
                        if(ring.size()==1){
                            f = ring.get(0);
                            Node newNode = new Node(NodeIDtoJoin, f, f, NodePort, NodePorttoJoin);
                            f.setSuccessor(newNode);
                            f.setPredecessor(newNode);
                            successor = newNode.port;
                            predecessor = newNode.port;
                            ring.add(newNode);
                            ring = UpdatePredSucc(ring);
                        }
                        else {
                            Node newNode = new Node(NodeIDtoJoin, null, null, NodePort, NodePorttoJoin);
                            ring.add(newNode);
                            ring = UpdatePredSucc(ring);
                        }
                        Log.d(TAG,"SIZE:::::"+ring.size());
                        /*
                        String str = "";

                        for (int i = 0; i < ring.size(); i++) {
                            Node n = ring.get(i);
                            str = str + "Update-";
                            str = str + n.getPortno() + ":" + n.getPredecessor().getPort() + ":" + n.getSuccessor().getPort() + "#";
                        }


                        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                        dos.writeBytes(str+"\n");
                        dos.flush();
                        */
                    } else if (msg.contains("Update")) {

                        String val[] = msg.split(":");
                        predecessor = val[1];
                        successor = val[2];

                    } else if (msg.contains("Insert")) {
                        String val[] = msg.split(":");
                        ContentValues cv = new ContentValues();
                        cv.put("key", val[1]);
                        cv.put("value", val[2]);
                        insert(mUri, cv);
                    } else if (msg.contains("ALLKVPAIRS*")) {
                        //https://stackoverflow.com/questions/10723770/whats-the-best-way-to-iterate-an-android-cursor
                        String kv = "";
                        Log.d(TAG, "Calling Cursor: Server Side");
                        Cursor c = query(mUri, null, "@", null, null, null);
                        try {
                            while (c.moveToNext()) {
                                Log.d(TAG, "Iterating over Cursor: Server Side");
                                String k = c.getString(c.getColumnIndex("key"));
                                String v = c.getString(c.getColumnIndex("value"));
                                kv += k;
                                kv += ":" + v;
                                kv += "|";
                            }

                        } finally {
                            c.close();
                        }
                        kv += "#" + successor;
                        Log.d(TAG, "Sending my Content Provider Data from Server Side" + kv);
                        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                        dos.writeBytes(kv +"\n");
                        dos.flush();
                        Log.d(TAG,"My key-value pairs sent"+myavdPort);

                    } else if (msg.contains("Query")) {
                        Log.d(TAG, "Reveived Query Request at::"+myavdPort);
                        String k = msg.split(":")[1];
                        //String hk = genHash(k);
                        //Check in Files to get key-value and send back
                        String data;
                        String found = "";
                        Context con = getContext();
                        for (int i = 0; i < Files.size(); i++) {
                            if (k.equals(Files.get(i))) {

                                try {
                                    FileInputStream fis = con.openFileInput(Files.get(i));
                                    BufferedReader br1 = new BufferedReader(new InputStreamReader(fis));
                                    while ((data = br1.readLine()) != null) {

                                        found += k;
                                        found += ":"+data;
                                        break;
                                    }
                                    fis.close();
                                } catch (FileNotFoundException e) {
                                    e.printStackTrace();
                                } catch (IOException e) {
                                    e.printStackTrace();


                                }
                            }
                        }
                        Log.d(TAG, "Server side:Checking in my files done");
                        if (found != "") {
                            Log.d(TAG, "Server side: Found Value");
                            //When Key found the msg1 -> Found#Key:Value
                            String msg1 = "Found#"+ found ;
                            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                            dos.writeBytes(msg1 +"\n");
                            dos.flush();
                            Log.d(TAG, "Server side:  Found Value: Sent");
                        }
                        else{
                            Log.d(TAG, "Server side: Not Found Value: Ask my Successor");
                            //When key not found then msg1-> NotFound#successor
                            String msg1 = "NotFound#"+String.valueOf(Integer.parseInt(successor)*2);
                            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                            dos.writeBytes(msg1 +"\n");
                            dos.flush();

                        }


                    }
                    else if(msg.contains("Delete")){

                        delete(mUri, "@", null);
                        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                        dos.writeBytes(successor +"\n");
                        dos.flush();

                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            return null;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void>{

        @Override
        protected Void doInBackground(String... msgs) {
            Log.d(TAG, "hello");
            String msg = msgs[0];
            Log.d(TAG, "MSG:"+msg);
            String port = msgs[1];
            Log.d(TAG, "port"+port);
            try {


                if(msg.contains("JoinMe")) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                    dos.writeBytes(msg+"\n");
                    dos.flush();


                    InputStreamReader isr = new InputStreamReader(socket.getInputStream());
                    BufferedReader br = new BufferedReader(isr);
                    String s = br.readLine();
                    socket.close();
                    /*
                    String update[] = s.split("#");
                    // Updated Predecessor and Successor
                    for(int i =0; i<update.length;i++){
                        String val[] = update[i].split("-")[1].split(":");
                        String f = update[i].split("-")[1];
                        Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(val[0]));
                        DataOutputStream dout = new DataOutputStream(socket1.getOutputStream());
                        dout.writeBytes("Update:"+f);
                    }*/

                }
                else if(msg.contains("Update")){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                    dos.writeBytes(msg+"\n");
                    dos.flush();
                    socket.close();

                }
                else if(msg.contains("Insert")){
                    Log.d(TAG,"Key by Predecessor to Insert:"+msg);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                    dos.writeBytes(msg+"\n");
                    dos.flush();
                    socket.close();
                }
                else if(msg.contains("ALLKVPAIRS*")){
                    Log.d(TAG, "Received Query type *");
                    String me = String.valueOf(Integer.parseInt(myavdPort)*2);
                    String send = String.valueOf(Integer.parseInt(successor)*2);

                    // Take all the key value pairs from the other nodes and add it to the arraylist which could be accessed from Query Method.
                    String collect ="";
                    while(!send.equals(me)){

                        Socket socketnew = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(send));
                        DataOutputStream dout = new DataOutputStream(socketnew.getOutputStream());
                        dout.writeBytes(msg +"\n");
                        dout.flush();

                        //Get Key-Value Pair From the Successor Cursor in String form with some delimiter as well as its successor and update the send port with successor.

                        InputStreamReader isr = new InputStreamReader(socketnew.getInputStream());
                        BufferedReader br = new BufferedReader(isr);
                        String kvps = br.readLine();
                        String a[] = kvps.split("#");
                        if(a.length==1){
                            send = String.valueOf(Integer.parseInt(a[0])*2);
                        }
                        else {
                            send = String.valueOf(Integer.parseInt(a[1]) * 2);

                            if (a[0] != "")
                                collect += a[0];
                        }
                        socketnew.close();

                    }

                    AllKeyVal.put(collect);

                }
                else if(msg.contains("Query")){
                    //String k = msg.split(":")[1];
                    Log.d(TAG,"Client:: Query Loop Start");
                    String me = String.valueOf(Integer.parseInt(myavdPort)*2);
                    String send = String.valueOf(Integer.parseInt(successor)*2);
                    String collect ="";
                    while(!send.equals(me)){
                        Log.d(TAG,"Sent Message to port::"+send);
                        Socket socketnew = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(send));
                        DataOutputStream dout = new DataOutputStream(socketnew.getOutputStream());
                        dout.writeBytes(msg + "\n");
                        dout.flush();

                        //Get Key-Value Pair From the Successor Cursor in String form with some delimiter as well as its successor and update the send port with successor.

                        InputStreamReader isr = new InputStreamReader(socketnew.getInputStream());
                        BufferedReader br = new BufferedReader(isr);
                        String msg1 = br.readLine();

                        if(msg1.split("#")[0].equals("Found")){
                            Log.d(TAG,"Found in port port::"+send);
                            String kv = msg1.split("#")[1];
                            QueryKeyVal.put(kv);
                            send = me;
                            break;
                        }
                        else if(msg1.split("#")[0].equals("NotFound")){
                            send = msg1.split("#")[1];

                        }
                    socketnew.close();

                    }


                }
                else if(msg.contains("Deleteall")){
                    String me = String.valueOf(Integer.parseInt(myavdPort)*2);
                    String send = String.valueOf(Integer.parseInt(successor)*2);

                    while(send!=me){
                        Socket socketnew = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(send));
                        DataOutputStream dout = new DataOutputStream(socketnew.getOutputStream());
                        dout.writeBytes("Delete" + "\n");
                        dout.flush();


                        InputStreamReader isr = new InputStreamReader(socketnew.getInputStream());
                        BufferedReader br = new BufferedReader(isr);
                        String msg1 = br.readLine();
                        send = msg1;

                    }


                }

            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return null;
        }
    }
}