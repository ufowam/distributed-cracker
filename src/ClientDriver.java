import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;

import org.apache.zookeeper.KeeperException;


public class ClientDriver {
	
	public static ZkConnector zkc = null;
	public static String jobTrackerAddr = null;
	
	public static void main(String[] args) {
		if (args.length < 3) {
			System.out.println("Usage: java ClientDriver <zookeeper host>:<zookeper port> job|status <passwordHash>");
			return;
		}
		
		zkc = new ZkConnector();
        try {
            zkc.connect(args[0]);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
            return;
        }
        
        try {
			jobTrackerAddr = zkc.getData("/jobTrackerPrimary", null, null);
		} catch (KeeperException e) {
			e.printStackTrace();
			return;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return;
		}
        
        String command = args[1];
        String passwordHash = args[2];
        
        if ("job".equals(command)) {
        	System.out.println(createJob(passwordHash));
        } else if ("status".equals(command)) {
        	System.out.println(getStatus(passwordHash));
        } else {
        	System.out.println("Usage: java ClientDriver <zookeeper host>:<zookeper port> job|status <passwordHash>");
        }
	}
	
	public static String createJob(String passwordHash) {
		 String response = sendCommand("job " + passwordHash);
		 if ("OK".equals(response)) {
			 return "Job was successfully submitted.";
		 }
		 return "Something wrong happened while submitting the job, please try again.";
	}
	
	public static String getStatus(String passwordHash) {
		String response = sendCommand("status " + passwordHash);
		
		String params[] = response.split(" ");
		if ("inprogress".equals(params[0])) {
			return "In Progress";
		} else if ("found".equals(params[0])) {
			return "Password found: " + params[1];
		} else if ("fail".equals(params[0])) {
			return "Failed: " + params[1];
		}
		return "";
	}
	
	private static String sendCommand(String command) {
		String[] split = jobTrackerAddr.split(":");
		String host = split[0];
		int port = Integer.parseInt(split[1]);
		Socket client;
		try {
			client = new Socket(host, port);
			BufferedReader readStream;
			BufferedWriter writeStream;
			
			readStream = new BufferedReader(new InputStreamReader(client.getInputStream()));
			writeStream = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));
			
			String str = command + "\n";
			writeStream.write(str, 0, str.length());
			writeStream.flush();
			
			int value = 0;
			StringBuilder buf = new StringBuilder();
			while((value = readStream.read()) != -1)
	        {
	            // converts int to character
	            buf.append((char)value);
	        }
			
			String resp = buf.toString();
			client.close();
			return resp;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "";
	}

}
