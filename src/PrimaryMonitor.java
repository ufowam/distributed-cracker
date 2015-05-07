import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

public class PrimaryMonitor implements Runnable {
    
    private final String primaryPath;
    private final ZkConnector zkc;
    private final Watcher watcher;
    private final String address;

    public PrimaryMonitor(ZkConnector zkc, String primaryPath, String address) {
    	this.primaryPath = primaryPath;
    	this.address = address;
    	this.zkc = zkc;
        
        watcher = new Watcher() { // Anonymous Watcher.
	        @Override
	        public void process(WatchedEvent event) {
	            handleEvent(event);
	    
	    }   };
    }
    
    /**
     * 
     */
    private void monitor() {
        Stat stat = zkc.exists(primaryPath, watcher);
        if (stat == null) {              // Znode doesn't exist; let's try creating it.
            Code ret = zkc.create(
                        primaryPath,         // Path of znode.
                        address,           // Data not needed.
                        CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
                        );
            if (ret == Code.OK) System.out.println("Node set to Primary.");
        } 
    }

    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(primaryPath)) {
            if (type == EventType.NodeDeleted) {
            	// Primary disappeared, try to become the new primary.
                monitor(); 
            }
            if (type == EventType.NodeCreated) {
                monitor(); // Re-enable the watch.
            }
        }
    }

	@Override
	public void run() {
		monitor();
		while (true) {
            try{ Thread.sleep(1); } catch (Exception e) {}
        }
	}

}
