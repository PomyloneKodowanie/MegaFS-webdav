package net.sf.webdav;

import javax.xml.bind.DatatypeConverter;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.CookieHandler;
import java.net.CookieManager;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Database {
    private Connection connection;
    private Statement statement;
    final String megatoolsBinary = "megatools";


    public Database() {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            System.out.println("Sqlite driver classfile not found: " + e.getMessage());
        }
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:MegaFS.db");
            statement = connection.createStatement();
        } catch (SQLException e) {
            System.out.println("Error opening database connection");
            System.out.println(e.getMessage());
        }
        createTables();
    }

    public boolean createTables() {
        String createAccounts = "CREATE TABLE IF NOT EXISTS accounts (username varchar(255) PRIMARY KEY, password varchar(255), free_space INTEGER)";
        String createFilesystem = "CREATE TABLE IF NOT EXISTS filesystem (sha1 varchar(255) PRIMARY KEY, remote_path varchar(255), username varchar(255), file_size INTEGER)";
        try {
            statement.execute(createAccounts);
            statement.execute(createFilesystem);
        } catch (SQLException e) {
            System.out.println("Error creating tables");
            System.out.println(e.getMessage());
            return false;
        }
        return true;
    }

    public void upload(String localPath) {
        File fileHandle = new File(localPath);
        if (localPath == null || localPath.isEmpty() || !fileHandle.exists()) {
            System.out.println("File name can't be empty or file doesn't exist");
            return;
        }
        long fileSize = checkFileSize(localPath);
        String username = getAccountWithFreeSpace(fileSize);
        String password = getPasswordWhereUsername(username);
        Process process;
        String sha1 = calcSHA1(localPath);

        if (isDuplicateHash(sha1)) {
            System.out.println("File " + localPath + " already exists in database");
            return;
        }
        try {
            List<String> cmdList = new ArrayList<String>();
            System.out.println("Uploading: " + localPath);
            cmdList.add(megatoolsBinary);
            cmdList.add("put");
            cmdList.add("--username=" + username);
            cmdList.add("--password=" + password);
            cmdList.add(localPath);
            ProcessBuilder processBuilder = new ProcessBuilder();
            processBuilder.command(cmdList);
            processBuilder.inheritIO();
            processBuilder.redirectErrorStream(true);
            process = processBuilder.start();
            process.waitFor();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            String lastLine = "";
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                lastLine = line;
            }
            if (lastLine.contains("File already exists")) {
                String remotePath = lastLine.substring(lastLine.lastIndexOf(" ") + 1);
                System.out.println("File already exists at remote Path: " + remotePath);
                return;
            } else if (lastLine.contains("Upload failed for")) {
                System.out.println(lastLine);
                return;
            }
        } catch (IOException e) {
            System.out.println("Process couldn't start while uploading: " + e.getMessage());
            return;
        } catch (InterruptedException e) {
            System.out.println("Process got interrupted while uploading: " + e.getMessage());
            return;
        }
        insertNewFile(sha1, localPath, username, fileSize);
        updateDifferenceOfFreeSpaceForAccount(username, fileSize);
    }

    public void download(String remotePath, String localPath) {
        String username = getUsernameWherePath(remotePath);
        String password = getPasswordWhereUsername(username);
        Process process;
        System.out.println("Downloading " + remotePath + " to: " + localPath);
        try {
            List<String> cmdList = new ArrayList<String>();
            cmdList.add(megatoolsBinary);
            cmdList.add("get");
            cmdList.add("--username=" + username);
            cmdList.add("--password=" + password);
            cmdList.add(remotePath);
            cmdList.add("--path=" + localPath);
            ProcessBuilder processBuilder = new ProcessBuilder();
            processBuilder.command(cmdList);
            processBuilder.inheritIO();
            processBuilder.redirectErrorStream(true);
            process = processBuilder.start();
            process.waitFor();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            String lastLine = "";
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                lastLine = line;
            }
            if (lastLine.contains("Local file already exists")) {
                System.out.println(lastLine);
                return;
            }
        } catch (IOException e) {
            System.out.println("Process couldn't start" + e.getMessage());
        } catch (InterruptedException e) {
            System.out.println("Process got interrupted" + e.getMessage());
        }
    }

    public void remove(String remotePath) {
        System.out.println("Removing " + remotePath);
        long fileSize = getFileSize(remotePath);
        String username = getUsernameWherePath(remotePath);
        String password = getPasswordWhereUsername(username);
        Process process;
        try {
            List<String> cmdList = new ArrayList<String>();
            cmdList.add(megatoolsBinary);
            cmdList.add("rm");
            cmdList.add("--username=" + username);
            cmdList.add("--password=" + password);
            cmdList.add(remotePath);
            ProcessBuilder processBuilder = new ProcessBuilder();
            processBuilder.command(cmdList);
            processBuilder.redirectErrorStream(true);
            process = processBuilder.start();
            process.waitFor();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            String lastLine = "";
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                lastLine = line;
            }
//            if (lastLine.contains("Local file already exists")) {
//                System.out.println(lastLine);
//                return;
//            }
        } catch (IOException e) {
            System.out.println("Process couldn't start" + e.getMessage());
            return;
        } catch (InterruptedException e) {
            System.out.println("Process got interrupted" + e.getMessage());
            return;
        }
        removeFromDatabase(remotePath);
        updateSumOfFreeSpaceForAccount(username, fileSize);
    }

    private boolean removeFromDatabase(String remotePath) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    "DELETE FROM filesystem WHERE remote_path = ?");
            preparedStatement.setString(1, remotePath);
            preparedStatement.execute();
            return true;
        } catch (SQLException e) {
            System.out.println("Error deleting file from database " + e.getMessage());
            return false;
        }
    }

    private String getUsernameWherePath(String remotePath) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    "SELECT username FROM filesystem WHERE remote_path = ?");
            preparedStatement.setString(1, remotePath);
            ResultSet resultSet = preparedStatement.executeQuery();
            String username = resultSet.getString("username");
            return username;
        } catch (SQLException e) {
            System.out.println("Error getting username for remote path: " + e.getMessage());
            return null;
        }
    }

    private void updateDifferenceOfFreeSpaceForAccount(String username, long fileSize) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    "SELECT free_space FROM accounts WHERE username = ?");
            preparedStatement.setString(1, username);
            ResultSet resultSet = preparedStatement.executeQuery();
            long freeSpace = resultSet.getLong("free_space");
            freeSpace -= fileSize;
            preparedStatement = connection.prepareStatement(
                    "UPDATE accounts SET free_space = ? WHERE username = ?");
            preparedStatement.setLong(1, freeSpace);
            preparedStatement.setString(2, username);
            preparedStatement.execute();
        } catch (SQLException e) {
            System.out.println("Error updating free space left: " + e.getMessage());
        }
    }

    private void updateSumOfFreeSpaceForAccount(String username, long fileSize) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    "SELECT free_space FROM accounts WHERE username = ?");
            preparedStatement.setString(1, username);
            ResultSet resultSet = preparedStatement.executeQuery();
            long freeSpace = resultSet.getLong("free_space");
            freeSpace += fileSize;
            preparedStatement = connection.prepareStatement(
                    "UPDATE accounts SET free_space = ? WHERE username = ?");
            preparedStatement.setLong(1, freeSpace);
            preparedStatement.setString(2, username);
            preparedStatement.execute();
        } catch (SQLException e) {
            System.out.println("Error updating free space left: " + e.getMessage());
        }
    }

//    public String calcSHA1_old(File file) {
//        System.out.println("Calculating sha-1");
//        String sha1 = null;
//        MessageDigest digest = null;
//        try {
//            digest = MessageDigest.getInstance("SHA-1");
//        } catch (NoSuchAlgorithmException e1) {
//            System.out.println("Impossible to get SHA-1 digester" + e1);
//        }
//        try (InputStream input = new FileInputStream(file);
//             DigestInputStream digestStream = new DigestInputStream(input, digest)) {
//            while (digestStream.read() != -1) {
//                // read file stream without buffer
//            }
//            MessageDigest msgDigest = digestStream.getMessageDigest();
//            sha1 = new HexBinaryAdapter().marshal(msgDigest.digest());
//        } catch (IOException e) {
//            System.out.println("Error reading digest stream" + e.getMessage());
//        }
//        System.out.println(sha1);
//        return sha1;
//    }

    public boolean insertNewFile(String sha1, String localPath, String username, long fileSize) {
        String fileName = new File(localPath).getName();
        String remotePath = "/Root/" + fileName;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    "INSERT INTO filesystem VALUES (?, ?, ?, ?);");
            preparedStatement.setString(1, sha1);
            preparedStatement.setString(2, remotePath);
            preparedStatement.setString(3, username);
            preparedStatement.setLong(4, fileSize);
            preparedStatement.execute();
            return true;
        } catch (SQLException e) {
            System.out.println("Error inserting new file into database " + e.getMessage());
            return false;
        }
    }

    public boolean isDuplicateHash(String sha1) {
        //true means the file already exists or error occurred and file shouldn't be uploaded
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    "SELECT sha1 FROM filesystem WHERE sha1  = ?");
            preparedStatement.setString(1, sha1);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next() == false) {
                return false;
            } else {
                return true;
            }
        } catch (SQLException e) {
            System.out.println("Error checking for duplicate hash " + e.getMessage());
            return true;
        }

    }

    public String calcSHA1(String filename) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            md.update(Files.readAllBytes(Paths.get(filename)));
            byte[] digest = md.digest();
            String myChecksum = DatatypeConverter
                    .printHexBinary(digest).toUpperCase();
            return myChecksum;
        } catch (NoSuchAlgorithmException e) {
            System.out.println("No such algorithm exception while calculating hash: " + e.getMessage());
            return null;
        } catch (IOException e) {
            System.out.println("Error reading file while hashing: " + e.getMessage());
            return null;
        }
    }

    public long checkFileSize(String filename) {
        File file = new File(filename);
        return file.length();
    }

    public String getAccountWithFreeSpace(long fileSize) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    "SELECT username FROM accounts WHERE ? < free_space");
            preparedStatement.setLong(1, fileSize);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next() == false) {
                System.out.println("Creating a new account");
                createNewMegaAccount();
                preparedStatement = connection.prepareStatement(
                        "SELECT username FROM accounts WHERE ? < free_space");
                preparedStatement.setLong(1, fileSize);
                resultSet = preparedStatement.executeQuery();
            }
            String username = resultSet.getString("username");
            return username;
        } catch (SQLException e) {
            System.out.println("Error getting account with free space: " + e.getMessage());
            return null;
        }
    }

    public String getPasswordWhereUsername(String username) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    "SELECT password FROM accounts WHERE username = ?");
            preparedStatement.setString(1, username);
            ResultSet resultSet = preparedStatement.executeQuery();
            String password = resultSet.getString("password");
            return password;
        } catch (SQLException e) {
            System.out.println("Error getting password for username: " + e.getMessage());
            return null;
        }
    }

    public void createNewMegaAccount() {
        try {
            //first connection get e-mail
            CookieManager cookieManager = new CookieManager();
            CookieHandler.setDefault(cookieManager);
            HttpClient httpClient = HttpClient.newBuilder().cookieHandler(cookieManager).build();
            HttpRequest httpRequest = HttpRequest.newBuilder(URI.create("https://10minutemail.net/")).build();
            HttpResponse<String> httpResponse = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            String responseText = httpResponse.body();
            String e_mail = responseText.substring(responseText.indexOf("class=\"mailtext\" value=\"") + 24, responseText.indexOf("class=\"mailtext\" value=\"") + 42);
            // Generate password and verification code for a  new mega account
            String password = new Random().ints(13, 33, 122).collect(StringBuilder::new,
                    StringBuilder::appendCodePoint, StringBuilder::append)
                    .toString();
            String verifyCode = getVerifyCodeForNewAccount(e_mail, password);
            System.out.println("Sleep, waiting for email 90 seconds");
            //wait for e-mail to arrive and extract readmail value of Mega registration email
            TimeUnit.SECONDS.sleep(90);
            httpRequest = HttpRequest.newBuilder(URI.create("https://10minutemail.net/")).build();
            httpResponse = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            responseText = httpResponse.body();
            String readMail = responseText.substring(responseText.indexOf("readmail.html?mid=") + 18, responseText.indexOf("readmail.html?mid=") + 24);
            //Potentially make loop to wait even longer when e-mail hasn't arrived
            if (readMail.equals("welcom")) {
                System.out.println("Registration e-mail hasn't arrived in time sleeping 90 seconds longer");
                TimeUnit.SECONDS.sleep(90);
                httpRequest = HttpRequest.newBuilder(URI.create("https://10minutemail.net/")).build();
                httpResponse = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
                responseText = httpResponse.body();
                readMail = responseText.substring(responseText.indexOf("readmail.html?mid=") + 18, responseText.indexOf("readmail.html?mid=") + 24);
            }
            //Read Mega registration e-mail and extract the activation link
            httpRequest = HttpRequest.newBuilder(URI.create("https://10minutemail.net/readmail.html?mid=" + readMail)).build();
            httpResponse = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            responseText = httpResponse.body();
            String confirmationLink = responseText.substring(responseText.indexOf("#confirm") - 16, responseText.indexOf("#confirm") + 112);
            boolean verificationSuccessful = verifyNewAccount(verifyCode, confirmationLink);
            if (verificationSuccessful) {
                insertNewMegaAccount(e_mail, password);
            }
        } catch (IOException e) {
            System.out.println("Http exception: " + e.getMessage());
        } catch (InterruptedException e) {
            System.out.println("Http interrupted exception: " + e.getMessage());
        }
    }

    private boolean insertNewMegaAccount(String e_mail, String password) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    "INSERT INTO accounts VALUES (?,?, 53687091200);");
            preparedStatement.setString(1, e_mail);
            preparedStatement.setString(2, password);
            preparedStatement.execute();
            return true;
        } catch (SQLException e) {
            System.out.println("Error inserting new file into database " + e.getMessage());
            return false;
        }
    }

    private boolean verifyNewAccount(String verifyCode, String confirmationLink) {
        Process process;
        try {
            List<String> cmdList = new ArrayList<String>();
            cmdList.add(megatoolsBinary);
            cmdList.add("reg");
            cmdList.add("--verify");
            cmdList.add(verifyCode);
            cmdList.add(confirmationLink);
            ProcessBuilder processBuilder = new ProcessBuilder();
            processBuilder.command(cmdList);
            processBuilder.redirectErrorStream(true);
            process = processBuilder.start();
            process.waitFor();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            String lastLine = "";
            List<String> output = new ArrayList<String>();
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                output.add(line);
                lastLine = line;
            }
            if (lastLine.contains("Account registered successfully!")) {
                return true;
            } else {
                System.out.println("Error confirming the account");
                for (String s : output) {
                    System.out.println(s);
                }
                return false;
            }

        } catch (IOException e) {
            System.out.println("Process couldn't start" + e.getMessage());
        } catch (InterruptedException e) {
            System.out.println("Process got interrupted" + e.getMessage());
        }
        return false;
    }

    private String getVerifyCodeForNewAccount(String e_mail, String password) {
        Process process;
        try {
            List<String> cmdList = new ArrayList<String>();
            cmdList.add(megatoolsBinary);
            cmdList.add("reg");
            cmdList.add("--scripted");
            cmdList.add("--register");
            cmdList.add("--name=test");
            cmdList.add("--email=" + e_mail);
            cmdList.add("--password=" + password);
            ProcessBuilder processBuilder = new ProcessBuilder();
            processBuilder.command(cmdList);
            processBuilder.redirectErrorStream(true);
            process = processBuilder.start();
            process.waitFor();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            String lastLine = "";
            String verifyCode = "";
            List<String> output = new ArrayList<String>();
            while ((line = reader.readLine()) != null) {
                output.add(line);
                lastLine = line;
            }
            if (lastLine.contains("verify")) {
                verifyCode = lastLine.substring(lastLine.lastIndexOf("--verify ") + 9, lastLine.lastIndexOf("--verify ") + 70);
                return verifyCode;
            } else {
                System.out.println("Error getting verify line");
                for (String s : output) {
                    System.out.println(s);
                }
            }
        } catch (IOException e) {
            System.out.println("Process couldn't start" + e.getMessage());
        } catch (InterruptedException e) {
            System.out.println("Process got interrupted" + e.getMessage());
        }
        return null;
    }

    public List<String> getRemoteFileNames() {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    "SELECT remote_path FROM filesystem");
            ResultSet resultSet = preparedStatement.executeQuery();
            List<String> fileNames = new ArrayList<>();
            while (resultSet.next()) {
                String remotePath = resultSet.getString("remote_path");
                String remotePathSplit[] = remotePath.split("/");
                fileNames.add(remotePathSplit[remotePathSplit.length - 1]);
            }
            return fileNames;
        } catch (SQLException e) {
            System.out.println("Error getting remote file names: " + e.getMessage());
            return null;
        }
    }

    public long getFileSize(String remotePath) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    "SELECT file_size FROM filesystem WHERE remote_path = ?");
            preparedStatement.setString(1, remotePath);
            ResultSet resultSet = preparedStatement.executeQuery();
            long fileSize = resultSet.getLong("file_size");
            return fileSize;
        } catch (SQLException e) {
            System.out.println("Error getting file size: " + e.getMessage());
            return 0;
        }
    }
}
