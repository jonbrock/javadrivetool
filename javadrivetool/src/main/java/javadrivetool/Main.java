package javadrivetool;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.binary.Hex;

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
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.drive.model.ParentReference;
import com.google.gson.Gson;

public class Main {
	/** Global instance of the {@link FileDataStoreFactory}. */
	private static FileDataStoreFactory DATA_STORE_FACTORY;
	
	/** Global instance of the JSON factory. */
	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
	
	/** Global instance of the HTTP transport. */
	private static HttpTransport HTTP_TRANSPORT;
	
	/** Global instance of the scopes required by this quickstart. */
	private static final List<String> SCOPES = Arrays.asList(DriveScopes.DRIVE);
	
	private static Gson gson = new Gson();
	private static ByteBuffer byteBuffer = ByteBuffer.allocate(10000);
	
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
		
		Path root = null;
		Path config = null;
		
		if (line.hasOption("r")) root = Paths.get(line.getOptionValue("r"));
		if (line.hasOption("c")) config = Paths.get(line.getOptionValue("c"));
		
		/* Search for missing values. */
		if (root == null && config != null) root = config.normalize().getParent();
		if (root == null) root = findRoot(Paths.get(""));
		if (root == null) root = Paths.get("");
		if (config == null) config = root.resolve(".javadrivetool");
		
		args = line.getArgs();
		
		if (args == null || args.length == 0) {
			System.out.println("Missing command");
			System.exit(-1);
		}
		
		String cmd = args[0];
		
		switch (cmd) {
			case "init":
				init(config);
				break;
			case "pull":
				pull(config, root);
				break;
			case "push":
				push(config, root);
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
	
	public static Credential init(Path config) {
		try {
			// Load client secrets.
			InputStream in = Main.class.getResourceAsStream("/client_secrets.json");
			GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));
			
			// Build flow and trigger user authorization request.
			GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(getHttpTransport(), JSON_FACTORY, clientSecrets, SCOPES).setDataStoreFactory(getFileDataStoreFactory(config)).setAccessType("offline").build();
			Credential credential = new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
			
			return credential;
		}
		catch (Exception e) {
			System.exit(-1);
		}
		return null;
	}
	
	public static Drive getDriveService(Path config) {
		Credential credential = init(config);
		return new Drive.Builder(getHttpTransport(), JSON_FACTORY, credential).build();
	}
	
	
	
	public static boolean checkDuplicate(Path path, String id, String md5) {
		/* File doesn't exist, not a duplicate. */
		if (!Files.exists(path)) return false;
		
		JavaDriveToolMetaDataV1 metadata = null;
		try {
			UserDefinedFileAttributeView view = Files.getFileAttributeView(path, UserDefinedFileAttributeView.class);
			byteBuffer.clear();
			view.read("javadrivetool", byteBuffer);
			byteBuffer.flip();
			String data = StandardCharsets.UTF_8.decode(byteBuffer).toString();
			if (data != null && !data.isEmpty()) metadata = gson.fromJson(data, JavaDriveToolMetaDataV1.class);
		}
		catch (Exception e) {
		}
		
		/* Id's match, not a duplicate. */
		if (metadata != null && metadata.getFileId() != null && metadata.getFileId().equals(id)) return false;
		
		if (metadata == null) {
			try {
				System.out.println("Metadata is missing on " + path.toString() + ", computing md5 to check for match");
				MessageDigest md = MessageDigest.getInstance("MD5");
				ReadableByteChannel in = Files.newByteChannel(path);
				byteBuffer.clear();
				while (in.read(byteBuffer) != -1) {
					byteBuffer.flip();
					md.update(byteBuffer);
					byteBuffer.clear();
				}
				String fileMd5 = new String(Hex.encodeHex(md.digest()));
				if (fileMd5.equalsIgnoreCase(md5)) {
					System.out.println("md5 matches, adding metadata");
					/* We have the file locally without the metadata. Just add the metadata. */
					metadata = new JavaDriveToolMetaDataV1();
					metadata.setFileId(id);
					metadata.setMd5(md5);
					String data = gson.toJson(metadata);
					UserDefinedFileAttributeView view = Files.getFileAttributeView(path, UserDefinedFileAttributeView.class);
					view.write("javadrivetool", StandardCharsets.UTF_8.encode(data));
					return false;
				}
			}
			catch (Exception e) {
			}
		}
		
		return true;
	}
	
	public static Path resolveDuplicate(Path path, String id, String md5) {
		
		System.out.println("Duplicate detected: " + path.toString());
		String filename = path.getFileName().toString();
		int period = filename.lastIndexOf(".");
		String prefix, suffix;
		if (period > 0) {
			prefix = filename.substring(0, period);
			suffix = filename.substring(period);
		}
		else {
			prefix = filename;
			suffix = "";
		}
		for (int i = 1; i < 1000000000; i++) {
			String test = prefix + " (" + i + ")" + suffix;
			path = path.getParent().resolve(test);
			if (!Files.exists(path)) return path;
		}
		
		/* No way this will happen. */
		return path.getParent().resolve(prefix + " (" + UUID.randomUUID().toString() + ")" + suffix);
	}
	
	public static void pull(Path config, Path root) {
		Drive drive = getDriveService(config);
		PullService pull = new PullService(drive, root);
		pull.pull();
		
		/*
		
		try {
			String pageToken = null;
			Map<String, File> idToFileMap = new HashMap<>();
			Drive.Files.List request = service.files().list().setPageToken(pageToken).setMaxResults(100);
			
			do {
				FileList result = request.execute();
				for (File file : result.getItems()) {
					idToFileMap.put(file.getId(), file);
				}
				pageToken = result.getNextPageToken();
				request.setPageToken(pageToken);
			} while (pageToken != null && !pageToken.isEmpty());
			
			for (File file : idToFileMap.values()) {
				Path path = null;
				if (path == null) continue;
				
				path = root.resolve(path);
				
				String mimeType = file.getMimeType();
				if (mimeType != null && mimeType.equals("application/vnd.google-apps.folder")) {
					Files.createDirectories(path);
				}
				else {
					if (file.getDownloadUrl() == null) continue;
					
					Path parent = path.getParent();
					if (parent != null) {
						Files.createDirectories(parent);
						path = resolveDuplicate(path, file.getId(), file.getMd5Checksum());
						
						if (path == null) continue;
						
						JavaDriveToolMetaDataV1 metadata = new JavaDriveToolMetaDataV1();
						metadata.setFileId(file.getId());
						metadata.setMd5(file.getMd5Checksum());
						
						String data = gson.toJson(metadata);
						if (file.getDownloadUrl() != null) data += "\n" + file.getDownloadUrl();
						
						Files.write(path, data.getBytes());
						UserDefinedFileAttributeView view = Files.getFileAttributeView(path, UserDefinedFileAttributeView.class);
						view.write("javadrivetool", StandardCharsets.UTF_8.encode(file.getId()));
					}
				}
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		*/
	}
	
	public static void push(Path config, Path root) {
		
	}
}
