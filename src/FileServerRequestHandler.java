import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.List;


public class FileServerRequestHandler implements Runnable {
	private final Socket clientSocket;
	private final BufferedReader readStream;
	private final BufferedWriter writeStream;

	FileServerRequestHandler(Socket clientSocket) throws IOException {
		this.clientSocket = clientSocket;
		readStream = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
		writeStream = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
	}
	
	public void handleQuery(String query) { 
		try {
			int partitionIndex = Integer.parseInt(query);
			List<String> partition = FileServer.getPartition(partitionIndex);
			if (partition != null) {
				writeStream.write(partition.toString().replace("]", "").replace("[", ""));
				writeStream.flush();
			}
		} catch (NumberFormatException e) {
			return;
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
	}
	
	public void run() {
		try {
			String queryFromClient;
			if ((queryFromClient = readStream.readLine()) != null) {
				System.out.println("Request: " + queryFromClient);
				handleQuery(queryFromClient);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			close();
		}
	}
	
	public void close() {
		try {
			clientSocket.close();
			readStream.close();
			writeStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
