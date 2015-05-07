import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;


public class JobTrackerRequestHandler implements Runnable {
	private final Socket clientSocket;
	private final BufferedReader readStream;
	private final BufferedWriter writeStream;

	JobTrackerRequestHandler(Socket clientSocket) throws IOException {
		this.clientSocket = clientSocket;
		readStream = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
		writeStream = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
	}
	
	public void handleQuery(String query) {
		String[] querySplit = query.split(" ");
		if (querySplit.length < 2) {
			return;
		}
		
		String command = querySplit[0].trim();
		String passwordHash = querySplit[1].trim();
		String resp = "";
		if ("job".equals(command)) {
			if (JobTracker.createNewJob(passwordHash)) {
				resp = "OK";
			} else {
				resp = "FAIL";
			}
		} else if ("status".equals(command)) {
			resp = JobTracker.getJobStatus(passwordHash);
		}
		
		try {
			writeStream.write(resp);
			writeStream.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}

	@Override
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
