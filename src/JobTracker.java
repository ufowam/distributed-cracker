import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;


public class JobTracker {
	
	public static final double PARTITION_SIZE = 1000;
	public static final double DICT_SIZE = 265744;
	public static final String JOBS_ROOT = "/jobs";
	public static final String WORKERS_ROOT = "/workers";
	
	public static ZkConnector zkc = null;
	
	public static ServerSocket serverSocket = null;
	
	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("Usage: java JobTracker <zookeeper host>:<zookeper port>");
			return;
		}
		
		int port = args.length == 2 ? Integer.parseInt(args[2]) : 3000;
		initServer(port);
		
		if (serverSocket == null) return;
		
		zkc = new ZkConnector();
        try {
            zkc.connect(args[0]);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
            return;
        }
        
        Stat stat = zkc.exists(JOBS_ROOT, null);
        if (stat == null) {
        	zkc.create(JOBS_ROOT, null, CreateMode.PERSISTENT);
        }
        
    	try {
    		InetAddress ip;
			ip = InetAddress.getLocalHost();
			String address = ip.getHostName() + ":" + serverSocket.getLocalPort();
			(new Thread (new PrimaryMonitor(zkc, "/jobTrackerPrimary", address))).start();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		
		runServer();
	}
	
	public static void initServer(int port) {
		try {
			serverSocket = new ServerSocket(port);
		} catch (IOException e1) {
			e1.printStackTrace();
			return;
		}
	}
	
	public static void runServer() {
		/* Indefinitely accept new clients */
		while(true) {
			Socket clientSocket = null;
			try {
				clientSocket = serverSocket.accept();
				(new Thread (new JobTrackerRequestHandler(clientSocket))).start();
			} catch (IOException e) {
				System.err.println("Error: error accepting client connection");
				e.printStackTrace();
			}
		}
	}
	
	public static synchronized boolean createNewJob(String passwordHash) {
		int tasks = (int) Math.ceil(DICT_SIZE/PARTITION_SIZE);
		String jobPath = JOBS_ROOT + "/" + passwordHash;
		
		Stat stat = zkc.exists(jobPath, null);
		if (stat != null) {
			return true;
		}
		
		try {
			List<String> workers = zkc.zooKeeper.getChildren(WORKERS_ROOT, null);
			if (workers.isEmpty()) return false;
		} catch (KeeperException e) {
			return false;
		} catch (InterruptedException e) {
			return false;
		}
		
		Code ret;
		ret = zkc.create(
                jobPath,         // Path of znode
                "inprogress",           // Data not needed.
                CreateMode.PERSISTENT   // Znode type, set to EPHEMERAL.
                );
		if (ret != Code.OK) {
			return false;
		}
		
		System.out.println("Creating " + tasks + " tasks for job " + passwordHash);
		
		String taskPath;
		String taskData;
		for (int i = 1; i <= tasks; i++) {
			taskPath = jobPath + "/" + i;
			taskData = passwordHash + " " + i;
			ret = zkc.create(taskPath, taskData, CreateMode.PERSISTENT);
		}
		
		return true;
	}
	
	public static synchronized String getJobStatus(String passwordHash) {
		try {
			String data = zkc.getData(JOBS_ROOT + "/" + passwordHash, null, null);
			return data;
		} catch (KeeperException e) {
			if (e.code() == KeeperException.Code.NONODE) {
				return "fail notfound";
			}
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		return "";
	}

}
