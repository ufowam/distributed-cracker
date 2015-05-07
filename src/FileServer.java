import java.util.*;
import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

public class FileServer {
	
	public static final int PARTITION_SIZE = 1000;
	
	public static ZkConnector zkc = null;
	
	public static ServerSocket serverSocket = null;
	
	public static ArrayList<String> dictionary = new ArrayList<String>();
	
	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("Usage: java FileServer <zookeeper host>:<zookeper port> <dictionary path>");
			return;
		}
		
		System.out.println("Loading the dictionary.");
		try {
			initDictionary(args[1]);
		} catch (FileNotFoundException e) {
			System.err.println("Could not find the dictionary.");
			return;
		}
		
		System.out.println("Starting the server.");
		
		int port = args.length == 3 ? Integer.parseInt(args[2]) : 5000;
		initServer(port);
		
		if (serverSocket == null) return;
		
		// Connect to Zookeeper.
		zkc = new ZkConnector();
        try {
            zkc.connect(args[0]);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
            return;
        }
		
        // Start the primary/backup election and monitoring.
        try {
    		InetAddress ip;
			ip = InetAddress.getLocalHost();
			String address = ip.getHostAddress() + ":" + serverSocket.getLocalPort();
			(new Thread (new PrimaryMonitor(zkc, "/fileServerPrimary", address))).start();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return;
		}
		
		runServer();
	}
	
	/**
	 * Create the server socket.
	 * @param port
	 */
	public static void initServer(int port) {
		try {
			serverSocket = new ServerSocket(port);
		} catch (IOException e1) {
			e1.printStackTrace();
			return;
		}
	}
	
	/**
	 * Start accepting new client connections.
	 */
	public static void runServer() {
		// Indefinitely accept new clients.
		while(true) {
			Socket clientSocket = null;
			try {
				clientSocket = serverSocket.accept();
				(new Thread (new FileServerRequestHandler(clientSocket))).start();
			} catch (IOException e) {
				System.err.println("Error: error accepting client connection");
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Parse the dictionary file into a list of words.
	 * @param path The path to the dictionary file.
	 * @throws FileNotFoundException
	 */
	public static void initDictionary(String path) throws FileNotFoundException {
		File file = new File(path);
		BufferedReader reader = null;
		
		// Read the words from the dictionary file and create a list of strings
		// out of them.
		try {
			reader = new BufferedReader(new FileReader(file));
			String text = null;
			
			while ((text = reader.readLine()) != null) {
				dictionary.add(text);
			}	
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
		    try {
		        if (reader != null) {
		            reader.close();
		        }
		    } catch (IOException e) {
		    	throw new RuntimeException(e);
		    }
		}
	}
	
	/**
	 * Given a partition index, return the list of words from the dictionary
	 * that make up the partition.
	 * @param partition Index of the dictionary partition to retrieve.
	 * @return List<String> The list of words that make up the partition.
	 */
	public static List<String> getPartition(int partition) {
		int start = (partition - 1) * PARTITION_SIZE;
		if (start > dictionary.size() - 1) {
			// The partition index is out of range.
			return null;
		}
		
		int stop = Math.min(start + PARTITION_SIZE, dictionary.size());
		return dictionary.subList(start, stop);
	}
}
