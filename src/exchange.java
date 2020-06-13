import java.io.*;
import java.util.LinkedHashMap;
import java.util.Scanner;
import java.util.Map;
import java.util.Random;

/**
 * @author ypandya
 *
 */
public class exchange {
	
	public static final int PROCESS_WAIT_TIME = 5000;
	public static final int MASTER_WAIT_TIME = 10000;
	public static final String INPUT_FILE = "calls.txt";
	
	static LinkedHashMap<String, String []> communication = new LinkedHashMap<>();
	
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		
		loadFileData(INPUT_FILE);
		Thread master = new Thread(new Master(communication), "Master");
		master.start();
		
	}
	
	/**
	 * @param filename
	 * @throws IOException
	 */
	public static void loadFileData(String fname) throws IOException {
		
		Scanner data = null;
        try {
        	data = new Scanner(new File(fname));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        while (data.hasNextLine()) {
            String line = data.nextLine();
            line = line.trim();
            if (line != null && !line.isEmpty()){
            	String temp = "";
            	String sender = "";
            	String receivers = "";
            	temp = line.substring(line.indexOf("{")+1, line.indexOf("}"));
            	sender = temp.substring(0, line.indexOf(",")-1).trim();
            	receivers = temp.substring(line.indexOf(",")).trim();
            	receivers = receivers.substring(receivers.indexOf("[")+1, receivers.indexOf("]")).trim();
            	String [] receiverlist = receivers.split(",");
            	if ((sender != null || sender != "") && receiverlist.length > 0) {
            		communication.put(sender, receiverlist);
            	}
            }
        }
        
	}
	
}

/**
 * @author ypandya
 *
 */
class Master implements Runnable {
	
	static LinkedHashMap<String, String []> communication = new LinkedHashMap<>();
	static int counts = 0;
 
	Master(){};
	
	/**
	 * @param communication
	 */
	Master(LinkedHashMap<String, String []> communication){
		Master.communication = communication;
	}
	
	/**
	 *
	 */
	@Override
	public synchronized void run() {
		
		startupDisplay();
		
		try {
			initializeSlaveProcess();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * 
	 */
	public void startupDisplay() {
		System.out.println("** Calls to be made **");
		for (Map.Entry<String, String []> communicationObj : communication.entrySet()) {
			System.out.println(communicationObj.getKey() + ": " + "[" + String.join(",", communicationObj.getValue()) + "]");
		}
		System.out.println();
		
	}
	
	/**
	 * @throws InterruptedException
	 */
	public synchronized void initializeSlaveProcess() throws InterruptedException {
		
		for (Map.Entry<String, String []> communicationObj : communication.entrySet()) {
			Slave s = new Slave(communicationObj.getKey(), communicationObj.getValue());
			Slave.processList.put(communicationObj.getKey(), s);
		}
		
		for (Map.Entry<String, String []> communicationObj : communication.entrySet()) {
			try {
				Thread slave = new Thread(Slave.processList.get(communicationObj.getKey()));
				slave.start();
				Slave.slavethreadcount++;
			}catch(Exception e) {
				e.printStackTrace();
			}
		}
		
	}

	/**
	 * @param msg
	 */
	public void printSlaveProcessMessageData(Message msg) {
		System.out.println(msg);
	}
	
	/**
	 * @throws InterruptedException
	 */
	public void goodByeMaster() throws InterruptedException {
		System.out.println("\nMaster has received no replies for " + (exchange.MASTER_WAIT_TIME / 1000) + " seconds, ending...");	
	}
	
}

/**
 * @author ypandya
 *
 */
class Slave implements Runnable {
	
	public static LinkedHashMap<String,Slave> processList = new LinkedHashMap<String,Slave>();
	public String sender;
	public String[] receiverList;
	Master master = new Master();
	public static int slavethreadcount = 0;
	
	/**
	 * @param sender
	 * @param receiverList
	 */
	Slave(String sender, String[] receiverList){
		this.sender = sender;
		this.receiverList = receiverList;
	}
	
	/**
	 *
	 */
	@Override
	public synchronized void run() {
		try {
			this.initiateCommunication();
			wait(exchange.PROCESS_WAIT_TIME);
			this.goodByeSlave();
			Master.counts++;
			if (Master.counts == slavethreadcount) {
				synchronized(master) {
					Master.counts = 0;
					master.wait(exchange.MASTER_WAIT_TIME);
					master.goodByeMaster();
			    }
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @throws InterruptedException
	 */
	public void initiateCommunication() throws InterruptedException {
		for (String receiver : this.receiverList) {
			Thread.sleep(new Random().nextInt(100));
			this.generateintroMessage(receiver);
		}
	}
	
	/**
	 * @param user
	 * @throws InterruptedException
	 */
	public void generateintroMessage(String user) throws InterruptedException {
		Message msg = this.createMessage(user, "intro");
		master.printSlaveProcessMessageData(msg);
		Thread.sleep(new Random().nextInt(100));
		Slave p = processList.get(user);
		p.generatereplyMessage(this.sender);
	}
	
	/**
	 * @param user
	 */
	public void generatereplyMessage(String user) {
		Message msg = this.createMessage(user, "reply");
		master.printSlaveProcessMessageData(msg);
	}
	
	/**
	 * @param user
	 * @param message
	 * @return
	 */
	public Message createMessage(String user, String message) {
		Message msg = new Message();
		msg.setMessage(message);
		msg.setReceiver(user);
		msg.setSender(this.sender);
		msg.setTimestamp(System.currentTimeMillis());
		return msg;
	}
	
	/**
	 * 
	 */
	public void goodByeSlave() {
		System.out.println("\nProcess " + this.sender + " has received no calls for " + (exchange.PROCESS_WAIT_TIME / 1000) + " seconds, ending...");
	}

}