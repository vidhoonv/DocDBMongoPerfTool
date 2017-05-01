package mongodocdbperf;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.time.StopWatch;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class PerfTest {
	
	public static String mongoConnectionString;
	public static String mongoDBName;
	public static String mongoCollName;
	public static String sampleDocumentFile;
	public static String partitionKeyField;
	public static String userName;
	public static String password;
	public static String accEndpoint;
	
	public static boolean isPCollection;
	public static int numberOfDocumentsToInsert;
	public static int numThreads;
	public static int mongoPort;
	
	public static MongoClient client;
	public static MongoDatabase db;
	public static MongoCollection<Document> coll;
	
	public static Map<String, Object> sampleDocument;
	
	public static List<Thread> threads;
	
	public static AtomicInteger documentsInserted = new AtomicInteger(0);
	public static double rusConsumed = 0.0;
	
	public static void main(final String[] args) {
		PerfTest.ParseConfig();
		
		PerfTest.Setup();
		
		PerfTest.StartThreads();	
		
		PerfTest.Monitor();
	}
	
	private static void ParseConfig()
	{
		Properties props = new Properties();
		InputStream in;
		try {
			in = new FileInputStream(ClassLoader.getSystemClassLoader().getResource("config").getPath());
			props.load(in);
			
			//read and populate properties
			PerfTest.numThreads = Integer.parseInt(props.getProperty("ThreadCount"));
			PerfTest.numberOfDocumentsToInsert = Integer.parseInt(props.getProperty("NumberOfDocumentsToInsert"));
			PerfTest.mongoConnectionString = props.getProperty("ConnectionString");
			PerfTest.mongoDBName = props.getProperty("DatabaseName");
			PerfTest.mongoCollName = props.getProperty("CollectionName");
			PerfTest.isPCollection = Boolean.parseBoolean(props.getProperty("IsPCollection"));
			PerfTest.partitionKeyField = props.getProperty("CollectionPartitionKeyField");
			PerfTest.sampleDocumentFile = props.getProperty("DocumentTemplateFile");
			
			PerfTest.accEndpoint = props.getProperty("AccEndpoint");
			PerfTest.mongoPort = Integer.parseInt(props.getProperty("Port"));
			PerfTest.userName =  props.getProperty("UserName");
			PerfTest.password = props.getProperty("Password");
			
			in.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	//setup
	private static void Setup()
	{
		
		//get mongo client
		MongoClientOptions cliOpt = MongoClientOptions.builder()
				.connectionsPerHost(PerfTest.numThreads)
				.minConnectionsPerHost(PerfTest.numThreads)
				.threadsAllowedToBlockForConnectionMultiplier(1)
				.sslEnabled(true).build();
		
		MongoCredential cred = MongoCredential.createCredential(PerfTest.userName, PerfTest.mongoDBName, PerfTest.password.toCharArray());
		List<MongoCredential> credentials = new ArrayList<MongoCredential>();
		credentials.add(cred);
		
		ServerAddress servAddr = new ServerAddress(PerfTest.accEndpoint, PerfTest.mongoPort);
		PerfTest.client = new MongoClient(servAddr, credentials, cliOpt);
		PerfTest.client.dropDatabase(PerfTest.mongoDBName);
		PerfTest.db = PerfTest.client.getDatabase(PerfTest.mongoDBName);
		

		//create collection (single or partitioned based on config)
		if(PerfTest.isPCollection)
		{
			BsonDocument cmd = new BsonDocument();
			String fullCollName = PerfTest.mongoDBName+"."+PerfTest.mongoCollName;
			cmd.append("shardCollection", new BsonString(fullCollName));
			BsonDocument keyDoc = new BsonDocument();
			keyDoc.append(PerfTest.partitionKeyField, new BsonString("hashed"));
			cmd.append("key", keyDoc);
			PerfTest.db.runCommand(cmd);
			PerfTest.coll = PerfTest.db.getCollection(PerfTest.mongoCollName);
		}
		else
		{
			PerfTest.coll = PerfTest.db.getCollection(PerfTest.mongoCollName);
		}
		
	
		try {
			//read sample json document
			JSONParser parser = new JSONParser();
			String path  = ClassLoader.getSystemClassLoader().getResource(PerfTest.sampleDocumentFile).getPath();
			Object obj = parser.parse(new FileReader(path));
			JSONObject jsonObj = (JSONObject) obj;
			
			
			String jsonStr = jsonObj.toJSONString();
			//populate sample doc
			PerfTest.sampleDocument = new HashMap<String, Object>();
			ObjectMapper mapper = new ObjectMapper();
			PerfTest.sampleDocument = mapper.readValue(jsonStr, new TypeReference<Map<String, Object>>(){});
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void StartThreads()
	{
		PerfTest.threads = new ArrayList<Thread>(PerfTest.numThreads);
		for(int i=0;i<PerfTest.numThreads;i++)
		{
			Thread t = new Thread(new InsertDocument(PerfTest.db, PerfTest.coll, PerfTest.numberOfDocumentsToInsert, PerfTest.partitionKeyField, PerfTest.sampleDocument));
			PerfTest.threads.add(t);
			t.start();
		}
	}
	
	private static void Monitor()
	{
		StopWatch watch = StopWatch.createStarted();
		try
		{
			//periodically output data
			while(true)
			{
				
				Thread.sleep(1000);
				long activeThreads = 0;
	            for(Thread t : PerfTest.threads)
	            {
	            	if(t.isAlive())
	            	{
	            		activeThreads++;
	            	}
	            }
	            
	            long elapsed  = watch.getNanoTime()/1000000000;
	            int inserted = PerfTest.documentsInserted.get();
	            float insertsPerSec = (float)inserted/(float)elapsed;
	            
	            double rusPerSec = PerfTest.getRUs()/(double)elapsed;
	            
	            System.out.println(String.format("Time elapsed (s): %d, insertedCount: %d, Inserts per sec: %f RUs per sec: %f Active threads: %d", elapsed, inserted, insertsPerSec, rusPerSec, activeThreads));
	            
	            if(activeThreads < 0)
	            {
	            	System.out.println("No active threads");
	            	break;
	            }
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		
	}
	
	private static synchronized void addRUs(double ruConsumed)
	{
		PerfTest.rusConsumed += ruConsumed;
	}
	
	private static synchronized double getRUs()
	{
		return PerfTest.rusConsumed;
	}
	
	public static class InsertDocument implements Runnable {
		
		private MongoDatabase db;
		private MongoCollection<Document> coll;
		private int numDocsToInsert;
		private String pkey;
		private Map<String,Object> sampleDoc;
		private int backoffFactor;
		private static int backoffInterval = 1000;

		public InsertDocument(MongoDatabase db, MongoCollection<Document> coll, int numDocs, String pkey, Map<String,Object> sampleDoc) {
			this.db = db;
			this.coll =coll;
			this.numDocsToInsert = numDocs;
			this.pkey = pkey;
			this.sampleDoc = sampleDoc;
			this.backoffFactor = 0;
		}
		
		public void run() {	
			
			for(int i=0;i<this.numDocsToInsert;i++)
			{
				//create new doc
				Map<String, Object> nDoc = new HashMap<String, Object>(this.sampleDoc);
				String pval =  UUID.randomUUID().toString();
				nDoc.put(this.pkey, pval);
				//String id =  UUID.randomUUID().toString();
				//nDoc.put("_id", id);
				
				Document d = new Document(nDoc);	
				//safely insert doc
				try {
					this.InsertSafe(d);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return;
				}
			}
			
		}
		
		private void InsertSafe(Document doc) throws Exception {
			boolean retry = true;
			while(retry)
			{
				try
				{
					this.coll.insertOne(doc);
					
					PerfTest.documentsInserted.incrementAndGet();
					
					this.AccountRUs();
					
					if(this.backoffFactor > 1) {
						this.backoffFactor--;
					}
					return;
				}
				catch (MongoCommandException ex)
				{
					if(ex.getCode() == 16500)
					{
						//throttled - do backoff
						this.backoffFactor++;
						try {
							//System.out.println("Thread id: "+Thread.currentThread().getId()+" got throttled.. sleeping for... "+InsertDocument.backoffInterval * this.backoffFactor+"ms");
							Thread.sleep(InsertDocument.backoffInterval * this.backoffFactor);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				catch(Exception ex)
				{
					System.out.println("Thread id: "+Thread.currentThread().getId()+" failed unknown error.. exiting.. ");
					ex.printStackTrace();
					throw ex;
				}
			}
		}
		
		private void AccountRUs()
		{
			BsonDocument cmd = new BsonDocument();
			cmd.append("getLastRequestStatistics", new BsonInt32(1));
			Document response = this.db.runCommand(cmd);
			
			if(response.containsKey("CommandName"))
			{
				if(response.getString("CommandName").equals("insert"))
				{
					if(response.containsKey("RequestCharge"))
					{
						PerfTest.addRUs(response.getDouble("RequestCharge"));
					}
					else
					{
						System.out.println("request charge not found for operation");
					}	
				}
			}
		}

	}

}


