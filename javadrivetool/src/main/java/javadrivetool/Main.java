package javadrivetool;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;

public class Main {
	/** Global instance of the {@link FileDataStoreFactory}. */
	private static FileDataStoreFactory DATA_STORE_FACTORY;
	
	/** Global instance of the JSON factory. */
	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
	
	/** Global instance of the HTTP transport. */
	private static HttpTransport HTTP_TRANSPORT;
	
	/** Global instance of the scopes required by this quickstart. */
	private static final List<String> SCOPES = Arrays.asList(DriveScopes.DRIVE);
	
	public static void main(String[] args) {
		Options options = new Options();
		options.addOption("h", "help", false, "display usage information");
		options.addOption("r", "root", true, "local path to use as root of drive");
		options.addOption("c", "config", true, "local path to use as config directory");
		
		CommandLineParser parser = new DefaultParser();
		CommandLine line = null;
		try {
			line = parser.parse(options, args);
		}
		catch (ParseException e) {
			System.err.println("Error: " + e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("JavaDriveTool", options, true);
			System.exit(1);
			return;
		}
		
		if (line.hasOption("h") || args.length == 0) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("JavaDriveTool", options, true);
			return;
		}
		
		if (line.hasOption("r")) Config.root = Paths.get(line.getOptionValue("r"));
		if (line.hasOption("c")) Config.config = Paths.get(line.getOptionValue("c"));
		
		/* Search for missing values. */
		if (Config.root == null && Config.config != null) Config.root = Config.config.normalize().getParent();
		if (Config.root == null) Config.root = findRoot(Paths.get(""));
		if (Config.root == null) Config.root = Paths.get("");
		if (Config.config == null) Config.config = Config.root.resolve(".javadrivetool");
		
		args = line.getArgs();
		
		if (args == null || args.length == 0) {
			System.out.println("Missing command");
			System.exit(-1);
		}
		
		String cmd = args[0];
		
		switch (cmd) {
			case "init":
				init();
				break;
			case "pull":
				pull();
				break;
			case "push":
				push();
				break;
			default:
				System.out.println("Bad command: " + cmd);
		}
	}
	
	public static String encodeFilename(String filename) {
		/* Null. */
		if (filename == null || filename.isEmpty()) return null;
		
		filename = filename.replace("%", "%25");
		
		filename = filename.replace("<", "%3C");
		filename = filename.replace(">", "%3E");
		filename = filename.replace(":", "%3A");
		filename = filename.replace("\"", "%22");
		filename = filename.replace("/", "%2F");
		filename = filename.replace("\\", "%5C");
		filename = filename.replace("|", "%7C");
		filename = filename.replace("?", "%40");
		filename = filename.replace("*", "%2A");
		
		/* Filename can't end in a space or a period. */
		filename = filename.replaceAll(" $", "%20");
		
		/* This also takes care of "." and ".." special cases. */
		filename = filename.replaceAll("\\.$", "%2E");
		
		return filename;
	}
	
	public static Path findRoot(Path path) {
		Path root = path.normalize();
		while (root != null) {
			Path test = root.resolve(".javadrivetool");
			if (Files.exists(test)) break;
			root = root.getParent();
		}
		return root;
	}
	
	public static HttpTransport getHttpTransport() {
		if (HTTP_TRANSPORT == null) {
			try {
				HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
			}
			catch (Exception e) {
				System.exit(-1);
			}
		}
		
		return HTTP_TRANSPORT;
	}
	
	public static FileDataStoreFactory getFileDataStoreFactory(Path config) {
		if (DATA_STORE_FACTORY == null) {
			try {
				DATA_STORE_FACTORY = new FileDataStoreFactory(config.toFile());
			}
			catch (Exception e) {
				System.exit(-1);
			}
		}
		
		return DATA_STORE_FACTORY;
	}
	
	public static Credential init() {
		try {
			// Load client secrets.
			InputStream in = Main.class.getResourceAsStream("/client_secrets.json");
			GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));
			
			// Build flow and trigger user authorization request.
			GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(getHttpTransport(), JSON_FACTORY, clientSecrets, SCOPES).setDataStoreFactory(getFileDataStoreFactory(Config.config)).setAccessType("offline").build();
			Credential credential = new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
			
			return credential;
		}
		catch (Exception e) {
			System.exit(-1);
		}
		return null;
	}
	
	public static Drive getDriveService() {
		Credential credential = init();
		return new Drive.Builder(getHttpTransport(), JSON_FACTORY, credential).build();
	}
	
	public static void pull() {
		Drive drive = getDriveService();
		PullService pull = new PullService(drive, Config.root);
		pull.pull();
	}
	
	public static void push() {
		
	}
}
