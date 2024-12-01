import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

enum Command{
    READ,
    WRITE,
    JOIN,
    UPDATE
}

public class DataServer {
    private static int StoredInteger = 0;
    private static Socket socket   = null;
    private static ServerSocket    server   = null;
    // Used to store ports of all the backup servers so the primary can update them with values.
    private static ArrayList<Integer> BackupServerPorts = new ArrayList<Integer>();
    private static int PrimaryPort = -1;
    private static int BackupPort = -1;

    private static int getStoredInt(){
        return StoredInteger;
    }
    private static void setStoredInt(int newInteger) {
        StoredInteger = newInteger;
    }


    public static void main(String[] args) throws IOException {
        // if there is one port given then we can assume that we will be using the primary server
        if(args.length == 1) {
            PrimaryPort = Integer.parseInt(args[0]);
            PrimaryServer();
        }
        // if there is 2 args that means this is a backup server
        else if(args.length == 2) {
            BackupPort = Integer.parseInt(args[0]);
            PrimaryPort = Integer.parseInt(args[1]);
            BackupServer();
        }

    }

    public static void PrimaryServer() throws IOException {

        server = new ServerSocket(PrimaryPort);
        System.out.println("Primary server started at port " + PrimaryPort);
        while (true) {
            socket = server.accept();
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                // Read message from client
                String message = in.readLine();
                String[] commandArguments = message.split(":");
                // Command mapping
                Command command = null;
                switch (commandArguments[0]) {
                    case "READ" -> command = Command.READ;
                    case "WRITE" -> command = Command.WRITE;
                    case "JOIN" -> command = Command.JOIN;
                    case "UPDATE" -> command = Command.UPDATE;
                    default -> {
                        return;
                    }
                }

                /*
                Command execution section
                 */
                if (command == Command.READ) {
                    out.println(getStoredInt());
                } else if (command == Command.WRITE) {
                    setStoredInt(Integer.parseInt(commandArguments[1]));
                    for (int BackupServerPort : BackupServerPorts) {
                        Socket socketReplica = new Socket("localhost", BackupServerPort);

                        OutputStream output = socketReplica.getOutputStream();
                        PrintWriter writer = new PrintWriter(output, true);
                        InputStream input = socketReplica.getInputStream();
                        BufferedReader reader = new BufferedReader(new InputStreamReader(input));

                        writer.println("WRITE:" + StoredInteger);
                        socketReplica.close();
                    }
                    out.println("WRITTEN");
                } else if (command == Command.JOIN) {
                    int backupPort = Integer.parseInt(commandArguments[1]);
                    BackupServerPorts.add(backupPort);
                    out.println("JOINED");
                    System.out.println("Backup server successfully added: " + backupPort);
                } else if (command == Command.UPDATE) {
                    setStoredInt(Integer.parseInt(commandArguments[1]));

                    //send new value to all the backup servers
                    for (int BackupServerPort : BackupServerPorts) {
                        Socket socketReplica = new Socket("localhost", BackupServerPort);

                        OutputStream output = socketReplica.getOutputStream();
                        PrintWriter writer = new PrintWriter(output, true);
                        InputStream input = socketReplica.getInputStream();
                        BufferedReader reader = new BufferedReader(new InputStreamReader(input));

                        writer.println("UPDATE:" + StoredInteger);
                        socketReplica.close();
                    }
                    out.println("UPDATED");
                }
            }
    }

    public static void BackupServer() throws IOException {


        // connect with primary server
        server = new ServerSocket(BackupPort);
        Socket socketJoinPrimaryServer = new Socket("localhost",PrimaryPort);

        OutputStream outputJoinPrimaryServer = socketJoinPrimaryServer.getOutputStream();
        PrintWriter writerJoinPrimaryServer=new PrintWriter(outputJoinPrimaryServer,true);
        InputStream inputJoinPrimaryServer = socketJoinPrimaryServer.getInputStream();
        BufferedReader readerJoinPrimaryServer = new BufferedReader(new InputStreamReader(inputJoinPrimaryServer));

        writerJoinPrimaryServer.println("JOIN:"+BackupPort);
        String line = readerJoinPrimaryServer.readLine();
        if(!line.equals("JOINED")){
            return;
        }
        System.out.println("Successfully joined primary server network: " + PrimaryPort);
        System.out.println("Backup server started at port " + BackupPort);
        while(true) {
        socket = server.accept();

        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);


        // Read message from client
        String message = in.readLine();
        String[] commandArguments = message.split(":");
        // command mapping
        Command command = null;
        switch (commandArguments[0]) {
            case "READ" -> command = Command.READ;
            case "WRITE" -> command = Command.WRITE;
            case "UPDATE" -> command = Command.UPDATE;
            default -> {
                return;
            }
        }

                        /*
                Command execution section
                 */

        if(command == Command.READ) {
            out.println(getStoredInt());
        }
        else if(command == Command.WRITE) {
            setStoredInt(Integer.parseInt(commandArguments[1]));

            //send update to primary server
            socketJoinPrimaryServer = new Socket("localhost",PrimaryPort);

            OutputStream output = socketJoinPrimaryServer.getOutputStream();
            PrintWriter writer=new PrintWriter(output,true);
            InputStream input = socketJoinPrimaryServer.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));

            writer.println("UPDATE:"+StoredInteger);
            String lineRead = reader.readLine();
            if(!lineRead.equals("UPDATED")) {
                out.println("ERROR");
            }
            else{
                out.println("WRITTEN");
            }
        }
        else if (command == Command.UPDATE) {
            setStoredInt(Integer.parseInt(commandArguments[1]));
            out.println("UPDATED");
        }
        System.out.println(message);
        }
    }
}
