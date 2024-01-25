import java.io.*;
import java.net.Socket;

public class TectonicClient {
    private static final byte SUCCESS_BYTE = 0x1;

    private Socket conn;
    private String host;
    private String port;
    private String currentDB;

    public TectonicClient(String host, String port) throws IOException {
        this.host = host;
        this.port = port;
        connect();
    }

    private void connect() throws IOException {
        String addr = host + ":" + port;
        System.out.println("Connecting to " + addr);
        conn = new Socket(host, Integer.parseInt(port));
    }

    private void reconnect() throws IOException {
        if (conn != null) {
            conn.close();
        }
        for (int i = 0; i < 3; i++) {  // Retry 3 times
            try {
                Thread.sleep(2000);  // Sleep for 2 seconds before reconnecting
                connect();
                return;
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
        throw new IOException("Failed to reconnect after several attempts");
    }

    private void sendCommand(String command) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream()))) {
            int length = command.length();
            writer.write(String.format("%d%s", length, command));
            writer.flush();
        }
    }

    private String readResponse() throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            int success = reader.read();
            boolean isSuccess = success == SUCCESS_BYTE;

            // Read response size
            long size = readLong(reader);

            // Read response data
            char[] buf = new char[(int) size];
            reader.read(buf);

            String response = new String(buf);
            if (isSuccess) {
                return response;
            } else {
                throw new TectonicError(handleServerError(response));
            }
        }
    }

    private long readLong(BufferedReader reader) throws IOException {
        try {
            StringBuilder sizeStr = new StringBuilder();
            char c;
            while ((c = (char) reader.read()) != -1) {
                if (Character.isDigit(c)) {
                    sizeStr.append(c);
                } else {
                    break;
                }
            }
            return Long.parseLong(sizeStr.toString());
        } catch (NumberFormatException e) {
            throw new IOException("Failed to read response size", e);
        }
    }

    private String handleServerError(String response) {
        if (response.startsWith("ERR: DB")) {
            String[] parts = response.split(" ", 3);
            if (parts.length < 3) {
                return "DB error without book name";
            }
            String bookName = parts[2];
            return String.format("DBNotFoundError: %s", bookName);
        }
        return String.format("ServerError: %s", response);
    }

    public String cmd(String command) throws IOException {
        try {
            sendCommand(command);
            return readResponse();
        } catch (IOException e) {
            reconnect();  // Reconnect on error
            throw e;
        }
    }

    // Other methods similar to Go implementation...

    public static void main(String[] args) {
        try {
            TectonicClient client = new TectonicClient("localhost", "9001");
            String result = client.cmd("INFO");
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
