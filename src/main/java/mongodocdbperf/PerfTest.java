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
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;

public class PerfTest {
    
	public static String mongoDBName;
	public static String mongoCollName;
	public static String sampleDocumentFile;
	public static String partitionKeyField;
	public static String userName;
	public static String password;
	public static String accEndpoint;
	
	public static boolean isPCollection;
	public static boolean preProvisioned;
	public static int numberOfDocumentsToInsert;
	public static int batchsize;
	public static int numThreads;
	public static int mongoPort;
	
	public static MongoClient client;
	public static MongoDatabase db;
	public static MongoCollection<Document> coll;
	
	public static Map<String, Object> sampleDocument;
	
	public static List<Thread> threads;
	
	public static AtomicInteger documentsInserted = new AtomicInteger(0);
	
	public static AtomicInteger rusConsumed = new AtomicInteger(0);
	
	public static void main(final String[] args) {
		
		PerfTest.ParseConfig();

		PerfTest.PrintConfig();
		
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
			PerfTest.batchsize = Integer.parseInt(props.getProperty("BatchSize"));
			PerfTest.mongoDBName = props.getProperty("DatabaseName");
			PerfTest.mongoCollName = props.getProperty("CollectionName");
			PerfTest.isPCollection = Boolean.parseBoolean(props.getProperty("IsPCollection"));
			PerfTest.preProvisioned = Boolean.parseBoolean(props.getProperty("PreProvisioned"));
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
	
	private static void PrintConfig()
	{
		System.out.println("Summary:");
		System.out.println("---------------------------------------------------------------------");
		System.out.println("Endpoint: "+PerfTest.accEndpoint+":"+PerfTest.mongoPort);
		System.out.println("Collection: "+PerfTest.mongoDBName+"."+PerfTest.mongoCollName);
		System.out.println("Document Template: "+PerfTest.sampleDocumentFile);
		System.out.println("Degree of parallelism: "+PerfTest.numThreads);
		System.out.println("Batchsize: "+PerfTest.batchsize);
		System.out.println("---------------------------------------------------------------------");
		System.out.println();
		System.out.println("DocumentDBBenchmark starting...");
	}
	
	//setup
	private static void Setup()
	{
		System.out.println("Creating mongo client");
		//get mongo client
		MongoClientOptions cliOpt = MongoClientOptions.builder()
				.connectionsPerHost(PerfTest.numThreads)
				//.minConnectionsPerHost(PerfTest.numThreads)
				//.threadsAllowedToBlockForConnectionMultiplier(1)
				.sslEnabled(true).build();
		
		MongoCredential cred = MongoCredential.createCredential(PerfTest.userName, PerfTest.mongoDBName, PerfTest.password.toCharArray());
		List<MongoCredential> credentials = new ArrayList<MongoCredential>();
		credentials.add(cred);
		
		ServerAddress servAddr = new ServerAddress(PerfTest.accEndpoint, PerfTest.mongoPort);
		PerfTest.client = new MongoClient(servAddr, credentials, cliOpt);
		PerfTest.db = PerfTest.client.getDatabase(PerfTest.mongoDBName);
		PerfTest.coll = PerfTest.db.getCollection(PerfTest.mongoCollName);
		
		if(!PerfTest.preProvisioned)
		{
			PerfTest.coll.drop();
			
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//create collection (single or partitioned based on config)
			if(PerfTest.isPCollection)
			{
				System.out.println("Creating partitioned collection "+PerfTest.mongoCollName);
				
				BsonDocument cmd = new BsonDocument();
				String fullCollName = PerfTest.mongoDBName+"."+PerfTest.mongoCollName;
				cmd.append("shardCollection", new BsonString(fullCollName));
				BsonDocument keyDoc = new BsonDocument();
				keyDoc.append(PerfTest.partitionKeyField, new BsonString("hashed"));
				cmd.append("key", keyDoc);
				PerfTest.db.runCommand(cmd);
			}
			else
			{
				System.out.println("Creating collection "+PerfTest.mongoCollName);
			}
		}
		
		PerfTest.coll = PerfTest.db.getCollection(PerfTest.mongoCollName);
	
		System.out.println("Loading sample document "+PerfTest.sampleDocumentFile);
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
		System.out.println("Starting Inserts with "+PerfTest.numThreads+" threads");
		PerfTest.threads = new ArrayList<Thread>(PerfTest.numThreads);
		for(int i=0;i<PerfTest.numThreads;i++)
		{
			Thread t = new Thread(new InsertDocument(PerfTest.db, PerfTest.coll, Math.ceil((double)PerfTest.numberOfDocumentsToInsert/(double)PerfTest.numThreads), PerfTest.partitionKeyField, PerfTest.sampleDocument));
			PerfTest.threads.add(t);
			t.start();
		}
	}
	
	private static void Monitor()
	{
		StopWatch watch = StopWatch.createStarted();
		long elapsed  =  0;
		int inserted = 0;
		float insertsPerSec;
		double rusPerSec = 0.0;
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
	            
	            elapsed  = watch.getNanoTime()/1000000000;
	            inserted = PerfTest.documentsInserted.get();
	            insertsPerSec = (float)inserted/(float)elapsed;
	            
	            rusPerSec = PerfTest.getRUs()/(double)elapsed;
	            
	            System.out.println(String.format("Inserted %d docs @ %f writes/s, %f RU/s", inserted, insertsPerSec, rusPerSec));
	            
	            if(activeThreads <= 0)
	            {
	            	break;
	            }
			}
			
			System.out.println();
			System.out.println("Summary:");
			System.out.println("--------------------------------------------------------------------- ");
			System.out.println(String.format("Inserted %d docs @ %f writes/s, %f RU/s", inserted, insertsPerSec, rusPerSec));
			System.out.println("--------------------------------------------------------------------- ");
			System.out.println("DocumentDB Mongo Benchmark completed successfully.");
			
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		
	}
	
	private static  void addRUs(double ruConsumed)
	{
		PerfTest.rusConsumed.addAndGet((int)ruConsumed);
	}
	
	private static int getRUs()
	{
		return PerfTest.rusConsumed.get();
	}
	
	public static class InsertDocument implements Runnable {
		
		private MongoDatabase db;
		private MongoCollection<Document> coll;
		private double numDocsToInsert;
		private String pkey;
		private Map<String,Object> sampleDoc;
		private int backoffFactor;
		private static int backoffInterval = 1000;
		private boolean working;
		private long id;
		private double ru;

		public InsertDocument(MongoDatabase db, MongoCollection<Document> coll, double numDocs, String pkey, Map<String,Object> sampleDoc) {
			this.db = db;
			this.coll =coll;
			this.numDocsToInsert = numDocs;
			this.pkey = pkey;
			this.sampleDoc = sampleDoc;
			this.backoffFactor = 0;
			this.working = false;
		}
		
		public void run() {	
			
			this.id = Thread.currentThread().getId();	
			List<InsertOneModel<Document>> docs = new ArrayList<InsertOneModel<Document>>(PerfTest.batchsize);
			for(int i=0;i<this.numDocsToInsert;)
			{
				int j=0;
				
				while(j<PerfTest.batchsize)
				{
					Map<String, Object> nDoc = new HashMap<String, Object>(this.sampleDoc);
					Document d = new Document(nDoc);
					String pval =  UUID.randomUUID().toString();
					d.remove(this.pkey);
					d.put(this.pkey, pval);
					docs.add(new InsertOneModel<Document>(d));
					j++;
				}
				
				boolean res = true;
				//safely insert doc
				try {
					res = this.InsertSafe(docs, PerfTest.batchsize);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return;
				}
				
				docs = new ArrayList<InsertOneModel<Document>>(PerfTest.batchsize);
				if(res)
				{
					i+=batchsize;
				}
				
			}
		}
		
		private boolean InsertSafe(List<InsertOneModel<Document>> docs, int batchSize) throws Exception {			
			try
			{
				this.coll.bulkWrite(docs, new BulkWriteOptions().ordered(false));
				
				PerfTest.documentsInserted.addAndGet(batchSize);
				
				if(this.working == false)
				{
					this.AccountRUs();
					this.working = true;
				}
				
				PerfTest.addRUs(this.ru);
				
				if(this.backoffFactor > 1) {
					this.backoffFactor--;
				}
				return true;
			}
			catch(MongoSocketOpenException ex)
			{
				//connect timed out error
				Thread.sleep(5000);
				return false;
			}
			catch(MongoSocketWriteException ex)
			{
				//connection closed by server due to throttling
				Thread.sleep(5000);
				return false;
			}
			catch (MongoCommandException ex)
			{
				if(ex.getCode() == 16500)
				{
					//throttled - do backoff
					this.backoffFactor++;
					try {
						System.out.println("Thread id: "+this.id+" got throttled.. sleeping for... "+InsertDocument.backoffInterval * this.backoffFactor+"ms");
						Thread.sleep(InsertDocument.backoffInterval * this.backoffFactor);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					return false;
				}
				else
				{
					throw ex;
				}
				
			}
			catch(Exception ex)
			{
				System.out.println("Thread id: "+Thread.currentThread().getId()+" failed unknown error.. exiting.. ");				
				throw ex;
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
						this.ru = response.getDouble("RequestCharge");
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


