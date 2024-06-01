import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class Client {

    public static void main(String[] args) {
        String hostname = "192.248.22.133"; // The server's hostname or IP address
        int port = 9092; // The server's port number

        try (Socket socket = new Socket(hostname, port)) {
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

            // Sending data to the server
            Scanner scanner = new Scanner(System.in);
            System.out.println("Enter messages to send to the server (type 'exit' to quit):");

            String message;
            while (!(message = scanner.nextLine()).equalsIgnoreCase("exit")) {
                out.println(message);
            }

            System.out.println("Client disconnected.");
        } catch (Exception e) {
            System.out.println("Client exception: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
