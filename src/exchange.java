//Ref
//https://www.geeksforgeeks.org/multithreading-in-java/
//https://www.geeksforgeeks.org/synchronized-in-java/
//https://mincong.io/2018/07/01/method-execution-in-multithreading/
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
	 * Java main method to run the program
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		try {
			loadFileData(INPUT_FILE);
			Thread master = new Thread(new Master(communication));
			master.start();
		} catch (Exception e) {
			System.out.println("Something went wrong, please check file path");
		}
	}
	
	/**
	 * This method is used to read file data and load into LinkedHashMap
	 * @param filename name of the file
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

class Master implements Runnable {
	
	static LinkedHashMap<String, String []> communication = new LinkedHashMap<>();
	static int counts = 0;
 
	Master(){};
	
	/**
	 * Parameterized constructor
	 * @param communication LinkedHashMap object
	 */
	Master(LinkedHashMap<String, String []> communication){
		Master.communication = communication;
	}
	
	/**
	 * This method is used to run the master thread
	 */
	@Override
	public synchronized void run() {
		this.startupDisplay();
		try {
			this.initializeSlaveProcess();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * This method is used to display calling list in the master thread
	 */
	public void startupDisplay() {
		System.out.println("** Calls to be made **");
		for (Map.Entry<String, String []> communicationObj : communication.entrySet()) {
			System.out.println(communicationObj.getKey() + ": " + "[" + String.join(",", communicationObj.getValue()) + "]");
		}
		System.out.println();

	}
	
	/**
	 * This method is used to start the slave threads to run and communicate
	 * in between the slaves
	 * @throws InterruptedException
	 */
	public synchronized void initializeSlaveProcess() throws InterruptedException {
		for (Map.Entry<String, String []> communicationObj : communication.entrySet()) {
			Slave s = new Slave(communicationObj.getKey(), communicationObj.getValue());
			Slave.slavelist.put(communicationObj.getKey(), s);
		}
		
		for (Map.Entry<String, String []> communicationObj : communication.entrySet()) {
			try {
				Thread slave = new Thread(Slave.slavelist.get(communicationObj.getKey()));
				slave.start();
				Slave.slavethreadcount++;
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * This message is used to display message data on master thread
	 * @param msg Message object
	 */
	public void printSlaveProcessMessageData(Message msg) {
		System.out.println(msg);
	}
	
	/**
	 * This method is used to display goodbye message on master thread
	 * @throws InterruptedException
	 */
	public synchronized void goodByeMaster() throws InterruptedException {
		System.out.println("\nMaster has received no replies for " + (exchange.MASTER_WAIT_TIME / 1000) + " seconds, ending...");	
	}
	
}

class Slave implements Runnable {
	
	public static LinkedHashMap<String,Slave> slavelist = new LinkedHashMap<String,Slave>();
	public String sender;
	public String[] receiverList;
	Master master = new Master();
	public static int slavethreadcount = 0;
	
	/**
	 * Parameterized Constructor
	 * @param sender name of sender
	 * @param receiverList string list contains the receiver info
	 */
	Slave(String sender, String[] receiverList){
		this.sender = sender;
		this.receiverList = receiverList;
	}
	
	/**
	 * This method is used to run the slave threads
	 */
	@Override
	public synchronized void run() {
		try {
			Thread.sleep(new Random().nextInt(100));
			this.initiateCommunication();
			wait(exchange.PROCESS_WAIT_TIME);
			this.goodByeSlave();
			Master.counts++;
			if (Master.counts == slavethreadcount) {
				synchronized(master) {
					Master.counts = 0;
					master.wait(exchange.MASTER_WAIT_TIME-exchange.PROCESS_WAIT_TIME);
					master.goodByeMaster();
			    }
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * This method is used to communicate between slave threads
	 * @throws InterruptedException
	 */
	public void initiateCommunication() throws InterruptedException {
		for (String receiver : this.receiverList) {
			this.generateintroMessage(receiver);
		}
	}
	
	/**
	 * This method is used to generate message from sender and receiver
	 * @param user receiver name
	 * @throws InterruptedException
	 */
	public void generateintroMessage(String user) throws InterruptedException {
		long timestamp = System.nanoTime()/1000;
		Message msg = this.createMessage(user, "intro", timestamp);
		master.printSlaveProcessMessageData(msg);
		Thread.sleep(new Random().nextInt(100));
		Slave p = slavelist.get(user);
		p.generatereplyMessage(this.sender, timestamp);
	}
	
	/**
	 * This method is used to generate reply message
	 * @param user receiver name
	 * @param timestamp timestamp of the message
	 */
	public void generatereplyMessage(String user, long timestamp) {
		Message msg = this.createMessage(user, "reply", timestamp);
		master.printSlaveProcessMessageData(msg);
	}
	
	/**
	 * This method is used to create message object
	 * @param user receiver name
	 * @param message message text either intro or reply
	 * @param timestamp timestamp of the message
	 * @return message object
	 */
	public Message createMessage(String user, String message, long timestamp) {
		Message msg = new Message();
		msg.setMessage(message);
		msg.setReceiver(user);
		msg.setSender(this.sender);
		msg.setTimestamp(timestamp);
		return msg;
	}
	
	/**
	 * This method is used to display goodbye for slave thread
	 */
	public void goodByeSlave() {
		System.out.println("\nProcess " + this.sender + " has received no calls for " + (exchange.PROCESS_WAIT_TIME / 1000) + " seconds, ending...");
	}
	
}

class Message {

	public String sender;
	public String receiver;
	public String message;
	public Long timestamp;

	/**
	 * @return the sender
	 */
	public String getSender() {
		return sender;
	}

	/**
	 * @param sender the sender to set
	 */
	public void setSender(String sender) {
		this.sender = sender;
	}

	/**
	 * @return the receiver
	 */
	public String getReceiver() {
		return receiver;
	}

	/**
	 * @param receiver the receiver to set
	 */
	public void setReceiver(String receiver) {
		this.receiver = receiver;
	}

	/**
	 * @return the message
	 */
	public String getMessage() {
		return message;
	}

	/**
	 * @param message the message to set
	 */
	public void setMessage(String message) {
		this.message = message;
	}

	/**
	 * @return the timestamp
	 */
	public Long getTimestamp() {
		return timestamp;
	}

	/**
	 * @param timestamp the timestamp to set
	 */
	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * This method is used to generate display message statement based on
	 * message type sender and receiver
	 */
	@Override
	public String toString() {
		String returnmsg = getReceiver() + " received " + getMessage() + " message from " + getSender() + " [" + getTimestamp() + "]";
		return returnmsg;
	}
		
}