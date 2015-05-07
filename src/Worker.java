import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;


public class Worker {
	public static final String FILESERVER_PATH = "/fileServerPrimary";
	public static final String JOBS_ROOT = "/jobs";
	public static final String WORKERS_ROOT = "/workers";
	public static final String WORKING_ROOT = "/working";
	
	public static ZkConnector zkc = null;
	public static Watcher watcher = null;
	public static String fileServerAddr = "";
	
	static Integer mutex;
	
	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("Usage: java Worker <zookeeper host>:<zookeper port>");
			return;
		}
		
		zkc = new ZkConnector();
        try {
            zkc.connect(args[0]);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
            return;
        }
        
        init();
	}
	
	public static void init() {
		mutex = new Integer(-1);
		watcher = new Watcher() { // Anonymous Watcher
            @Override
            public void process(WatchedEvent event) {
                handleEvent(event);
        
            } };
		
		setFileServerAddr();
		
		Stat stat = zkc.exists(WORKERS_ROOT, null);
		if (stat == null) {
			zkc.create(WORKERS_ROOT, null, CreateMode.PERSISTENT);
		}
		
		zkc.create(WORKERS_ROOT + "/worker", null, CreateMode.EPHEMERAL_SEQUENTIAL);
		
		stat = zkc.exists(WORKING_ROOT, null);
		if (stat == null) {
			zkc.create(WORKING_ROOT, null, CreateMode.PERSISTENT);
		}
		
		while (true) {
			try {
				String task = consume();
				if (!"".equals(task)) {
					processTask(task);
				} else {
					synchronized(mutex) {
						zkc.zooKeeper.getChildren(JOBS_ROOT, watcher);
						mutex.wait();
					}
				}
			} catch (KeeperException e) {
			} catch (InterruptedException e) {
			}
		}
		
	}
	
	public static String consume() throws KeeperException, InterruptedException {
		List<String> jobs = zkc.zooKeeper.getChildren(JOBS_ROOT, null);
		for (String job : jobs) {
			String jobPath = JOBS_ROOT + "/" + job;
			String jobStatus = zkc.getData(jobPath, null, null);
			if ("inprogress".equals(jobStatus)) {
				List<String> tasks = zkc.zooKeeper.getChildren(jobPath, null);
				for (String task : tasks) {
					String taskPath = jobPath + "/" + task;
					String taskData = zkc.getData(taskPath, null, null);
					Code ret = zkc.create(WORKING_ROOT + "/" + job + "-" + task, null, CreateMode.EPHEMERAL);
					if (ret == Code.OK) {
						return taskData;
					}
				}
			}
		}
		
		return "";
	}
	
	public static void processTask(String task) throws KeeperException, InterruptedException {
		System.out.println("Processing task " + task);
		String[] params = task.split(" ");
		String passwordHash = params[0].trim();
		String password = "";
		int partitionId = Integer.parseInt(params[1]);
		
		ArrayList<String> partition = null;
		while (partition == null) {
			partition = getPartition(partitionId);
			if (partition == null) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		for (String word : partition) {
			String wordHash = getHash(word);
			if (passwordHash.equals(wordHash)) {
				password = word;
				break;
			}
		}
		
		String jobPath = JOBS_ROOT + "/" + passwordHash;
		if (!"".equals(password)) {
			System.out.println("FOUND PASSWORD!");
			
			String data = "found " + password;
			zkc.setData(jobPath, data);
		} else {
			List<String> children = zkc.zooKeeper.getChildren(jobPath, null);
			if (children.isEmpty()) {
				String data = "fail notfound";
				zkc.setData(jobPath, data);
			}
		}
		
		String taskPath = jobPath + "/" + partitionId;
		zkc.zooKeeper.delete(taskPath, 0);
		String workPath = WORKING_ROOT + "/" + passwordHash + "-" + partitionId;
		zkc.zooKeeper.delete(workPath, 0);
	}
	
	public static void setFileServerAddr() {
		try {
			fileServerAddr = zkc.getData(FILESERVER_PATH, watcher, null);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return;
		}
	}
	
	public static void handleEvent(WatchedEvent event) {
		String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(FILESERVER_PATH)) {
            if (type == EventType.NodeDeleted) {
                System.out.println("FileServer Deleted");
                fileServerAddr = "";
            }
            if (type == EventType.NodeCreated) {
            	System.out.println("FileServer Created");
            	try {
					fileServerAddr = zkc.getData(FILESERVER_PATH, watcher, null);
				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return;
				} catch (InterruptedException e) {
					e.printStackTrace();
					return;
				}
            }
        } else if (path.equals(JOBS_ROOT)) {
        	synchronized(mutex) {
        		mutex.notify();
        	}
        }
	}
	
	public static String getHash(String word) {

        String hash = null;
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            BigInteger hashint = new BigInteger(1, md5.digest(word.getBytes()));
            hash = hashint.toString(16);
            while (hash.length() < 32) hash = "0" + hash;
        } catch (NoSuchAlgorithmException nsae) {
            // ignore
        }
        return hash;
    }
	
	public static ArrayList<String> getPartition(int partitionId) {
		if ("".equals(fileServerAddr)) {
			setFileServerAddr();
			return null;
		}
		
		String[] split = fileServerAddr.split(":");
		String host = split[0];
		int port = Integer.parseInt(split[1]);
		try {
			Socket client = new Socket(host, port);
			BufferedReader readStream;
			BufferedWriter writeStream;
			
			readStream = new BufferedReader(new InputStreamReader(client.getInputStream()));
			writeStream = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));
			
			String str = Integer.toString(partitionId) + "\n";
			writeStream.write(str, 0, str.length());
			writeStream.flush();
			
			int value = 0;
			StringBuilder buf = new StringBuilder();
			while((value = readStream.read()) != -1)
	        {
	            // converts int to character
	            buf.append((char)value);
	        }
			
			String[] resp = buf.toString().split(",");
			ArrayList<String> partition = new ArrayList<String>();
			for (String word : resp) {
				partition.add(word.trim());
			}
			
			client.close();
			
			return partition;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
