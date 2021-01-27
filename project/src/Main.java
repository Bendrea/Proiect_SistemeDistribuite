import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import static java.lang.System.exit;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;


public class Main {
   public static final String TEXT_RESET = "\u001B[0m";
    public static final String TEXT_BLACK = "\u001B[30m";
    public static final String TEXT_RED = "\u001B[31m";
    public static final String TEXT_GREEN = "\u001B[32m";
    public static final String TEXT_YELLOW = "\u001B[33m";
    public static final String TEXT_BLUE = "\u001B[34m";
    public static final String TEXT_PURPLE = "\u001B[35m";
    public static final String TEXT_CYAN = "\u001B[36m";
    public static final String TEXT_WHITE = "\u001B[37m";
    static int regPort = Configurations.REG_PORT;

	static Registry registry ;

	static void respawnReplicaServers(Master master)throws IOException{
		System.out.println("[@main] respawning replica servers ");
		// TODO make file names global
		BufferedReader br = new BufferedReader(new FileReader("repServers.txt"));
		int n = Integer.parseInt(br.readLine().trim());
		ReplicaLoc replicaLoc;
		String s;

		for (int i = 0; i < n; i++) {
			s = br.readLine().trim();
			replicaLoc = new ReplicaLoc(i, s.substring(0, s.indexOf(':')) , true);
			ReplicaServer rs = new ReplicaServer(i, "./"); 

			ReplicaInterface stub = (ReplicaInterface) UnicastRemoteObject.exportObject(rs, 0);
			registry.rebind("ReplicaClient"+i, stub);

			master.registerReplicaServer(replicaLoc, stub);

			System.out.println("replica server state [@ main] = "+rs.isAlive());
		}
		br.close();
	}

	public static void launchClients(){
		try {
			Client c = new Client();
			char[] ss = "File 1 test test END ".toCharArray();
			byte[] data = new byte[ss.length];
			for (int i = 0; i < ss.length; i++) 
				data[i] = (byte) ss[i];

			c.write("file1", data);
			byte[] ret = c.read("file1");
			System.out.println("file1: " + ret);

			c = new Client();
			ss = "File 1 Again Again END ".toCharArray();
			data = new byte[ss.length];
			for (int i = 0; i < ss.length; i++) 
				data[i] = (byte) ss[i];

			c.write("file1", data);
			ret = c.read("file1");
			System.out.println("file1: " + ret);

			c = new Client();
			ss = "File 2 test test END ".toCharArray();
			data = new byte[ss.length];
			for (int i = 0; i < ss.length; i++) 
				data[i] = (byte) ss[i];

			c.write("file2", data);
			ret = c.read("file2");
			System.out.println("file2: " + ret);

		} catch (NotBoundException | IOException | MessageNotFoundException e) {
			e.printStackTrace();
		}
	}
	public  static void customTest() throws IOException, NotBoundException, MessageNotFoundException{
		Client c = new Client();
		String fileName = "file1";

		char[] ss = "[INITIAL DATA!]".toCharArray(); // len = 15
		byte[] data = new byte[ss.length];
		for (int i = 0; i < ss.length; i++) 
			data[i] = (byte) ss[i];

		c.write(fileName, data);

		c = new Client();
		ss = "File 1 test test END".toCharArray(); // len = 20
		data = new byte[ss.length];
		for (int i = 0; i < ss.length; i++) 
			data[i] = (byte) ss[i];

		
		byte[] chunk = new byte[Configurations.CHUNK_SIZE];

		int seqN =data.length/Configurations.CHUNK_SIZE;
		int lastChunkLen = Configurations.CHUNK_SIZE;

		if (data.length%Configurations.CHUNK_SIZE > 0) {
			lastChunkLen = data.length%Configurations.CHUNK_SIZE;
			seqN++;
		}
		
		WriteAck ackMsg = c.masterStub.write(fileName);
		ReplicaServerClientInterface stub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient"+ackMsg.getLoc().getId());

		FileContent fileContent;
		@SuppressWarnings("unused")
		ChunkAck chunkAck;
		//		for (int i = 0; i < seqN; i++) {
		System.arraycopy(data, 0*Configurations.CHUNK_SIZE, chunk, 0, Configurations.CHUNK_SIZE);
		fileContent = new FileContent(fileName, chunk);
		chunkAck = stub.write(ackMsg.getTransactionId(), 0, fileContent);


		System.arraycopy(data, 1*Configurations.CHUNK_SIZE, chunk, 0, Configurations.CHUNK_SIZE);
		fileContent = new FileContent(fileName, chunk);
		chunkAck = stub.write(ackMsg.getTransactionId(), 1, fileContent);

		// read here 
		List<ReplicaLoc> locations = c.masterStub.read(fileName);
		System.err.println("[@CustomTest] Read1 started ");

		// TODO fetch from all and verify 
		ReplicaLoc replicaLoc = locations.get(0);
		ReplicaServerClientInterface replicaStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient"+replicaLoc.getId());
		fileContent = replicaStub.read(fileName);
		System.err.println("[@CustomTest] data:");
		System.err.println(new String(fileContent.getData()));


		// continue write 
		for(int i = 2; i < seqN-1; i++){
			System.arraycopy(data, i*Configurations.CHUNK_SIZE, chunk, 0, Configurations.CHUNK_SIZE);
			fileContent = new FileContent(fileName, chunk);
			chunkAck = stub.write(ackMsg.getTransactionId(), i, fileContent);
		}
		// copy the last chuck that might be < CHUNK_SIZE
		System.arraycopy(data, (seqN-1)*Configurations.CHUNK_SIZE, chunk, 0, lastChunkLen);
		fileContent = new FileContent(fileName, chunk);
		chunkAck = stub.write(ackMsg.getTransactionId(), seqN-1, fileContent);

		
		
		//commit
		ReplicaLoc primaryLoc = c.masterStub.locatePrimaryReplica(fileName);
		ReplicaServerClientInterface primaryStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient"+primaryLoc.getId());
		primaryStub.commit(ackMsg.getTransactionId(), seqN);

		
		// read
		locations = c.masterStub.read(fileName);
		System.err.println("[@CustomTest] Read3 started ");

		replicaLoc = locations.get(0);
		replicaStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient"+replicaLoc.getId());
		fileContent = replicaStub.read(fileName);
		System.err.println("[@CustomTest] data:");
		System.err.println(new String(fileContent.getData()));

	}

	static Master startMaster() throws AccessException, RemoteException{
		Master master = new Master();
		MasterServerClientInterface stub = 
				(MasterServerClientInterface) UnicastRemoteObject.exportObject(master, 0);
		registry.rebind("MasterServerClientInterface", stub);
		System.err.println("Server ready");
		return master;
	}

	public static void main(String[] args) throws IOException {


		try {
			LocateRegistry.createRegistry(regPort);
			registry = LocateRegistry.getRegistry(regPort);

			Master master = startMaster();
			respawnReplicaServers(master);
                        
//			customTest();
			//launchClients();
                        System.out.println("\n");
                        interfataClient();

		} catch (RemoteException   e) {
			System.err.println("Server exception: " + e.toString());
			e.printStackTrace();
		}
	}
        
        private static  String readFromConsole(){
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String input = null;
            try{
                input = reader.readLine();
            }catch(IOException e){
                System.err.println("Can not read from console");
            }
            return input;
        }
        
        
        private static void interfataClient(){
            Client client = new Client(); //initializare clent nou catre server
            
            System.err.println("Clientul este online");
            
            System.out.println("Pentru a afisa fisierele disponibile tastati"+ TEXT_RED   + " dir" + TEXT_RESET );
            System.out.println("Pentru a citi un fisier  disponibil tastati" + TEXT_BLUE + " read" + TEXT_RESET);
            System.out.println("Pentru a scrie intr-un fisier trastati" + TEXT_CYAN + " write" + TEXT_RESET);
            System.out.println("Pentru un nou client tastati" + TEXT_GREEN +  " new" + TEXT_RESET);
            System.out.println("Daca doriti sa iesiti tastati" + TEXT_PURPLE +  " exit" + TEXT_RESET);
          
            
            while(true){
                switch(readFromConsole()){
                    case "dir":
                        CITIRE(); //citire cele 2 fisiere existente
                        break;
                    case "read":
                        citireFisier(client); //citire din fisier
                        break;
                    case "write":
                        scriereFisier(client); //scriere in fisier
                        break;
                    case "new":
                        interfataClient(); //inchid clientul existent si creez altu nou pe server
                        break;
                    case "exit":
                        exit(1); //inchidere conectiune si server
                        break;
                            
                }
            }
        }
        
        public static void CITIRE(){
            System.out.println("file1");
            System.out.println("file2");
        }
        
        //functia de citire in fisier
        public static void citireFisier(Client client){
            System.out.println("Inserati numele fisierului: ");
            String file = readFromConsole();
            wFile(client, file, " ");
            try{
                byte[] ret = client.read(file);
               
            }catch(IOException e){
                System.err.println("Can not read file" + file);
            }catch(NotBoundException e){
                System.err.println("Can not read file" + file);
            }
        }
        
        public static void wFile(Client client, String fileN, String text){
            char[] ss = text.toCharArray();
            byte[] data = new byte[ss.length];
            
            for (int i = 0; i < ss.length; i++) {
                data[i] = (byte) ss[i];
            }
            
            try{
                client.write(fileN, data);
            }catch(IOException e){
                System.err.println("Nu se poate scrie in fisier");
            }catch(NotBoundException e){
                System.err.println("Nu se poate scrie in fisier");
            }catch(MessageNotFoundException e){
                System.err.println("Nu se poate scrie in fisier");
            }            
        }
        
        public static void scriereFisier(Client client){
            System.out.println("insert file name");
            String file = readFromConsole();
            System.out.println("insert text");
            String text = readFromConsole();
            wFile(client, file, text);
        }
        
        public static void dFile(Client client, String fileN){
            
        }
        

}
