package javadrivetool;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.security.MessageDigest;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.codec.binary.Hex;

import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.gson.Gson;

public class PullService {
	private Path localRoot;
	
	private Drive drive;
	private ExecutorService listRemoteFilesService;
	private ExecutorService checkMd5Service;
	private ExecutorService downloadService;
	
	private static class LocalFileResolution {
		public boolean exists;
		public Path path;
		public JavaDriveToolMetaDataV1 metadata;
		
		public LocalFileResolution(boolean exists, Path path, JavaDriveToolMetaDataV1 metadata) {
			this.exists = exists;
			this.path = path;
			this.metadata = metadata;
		}
	}
	
	private class ListRemoteFilesTask implements Runnable {
		private String parentId;
		private Path parentPath;
		private ByteBuffer byteBuffer = ByteBuffer.allocate(10000);
		private Gson gson = new Gson();
		
		public ListRemoteFilesTask(String parentId, Path parentPath) {
			this.parentId = parentId;
			this.parentPath = parentPath;
		}
		
		public LocalFileResolution resolveLocalFile(File remoteFile, String filename) {
			String testFilename = filename;
			
			for (int iteration = 1; ; iteration++) {
				Path testPath = parentPath.resolve(testFilename);
				
				/* If it doesn't exist, we're done. */
				if (!Files.exists(testPath)) return new LocalFileResolution(false, testPath, null);
				
				/* If it does exist, see if it's the same as the remote file. */
				JavaDriveToolMetaDataV1 metadata = null;
				try {
					UserDefinedFileAttributeView view = Files.getFileAttributeView(testPath, UserDefinedFileAttributeView.class);
					byteBuffer.clear();
					view.read("javadrivetool", byteBuffer);
					byteBuffer.flip();
					String data = StandardCharsets.UTF_8.decode(byteBuffer).toString();
					if (data != null && !data.isEmpty()) metadata = gson.fromJson(data, JavaDriveToolMetaDataV1.class);
				}
				catch (Exception e) {
					metadata = null;
				}
				
				/* If there's no metadata, we're overwriting so we're done. */
				if (metadata == null || metadata.getFileId() == null) {
					System.out.println("Local file found without metadata, overwriting");
					return new LocalFileResolution(true, testPath, null);
				}
				
				/* If the file id matches, we're done. */
				if (metadata != null && metadata.getFileId() != null && metadata.getFileId().equals(remoteFile.getId())) {
					System.out.println("Local file found with matching metadata: " + testPath);
					return new LocalFileResolution(true, testPath, metadata);
				}
				
				/* Otherwise keep looking. */
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
				
				if (iteration > 10000) testFilename = prefix + " (" + UUID.randomUUID().toString() + ")" + suffix;
				else testFilename = prefix + " (" + iteration + ")" + suffix;
			}
		}
		
		public void run() {
			try {
				Drive.Files.List request = drive.files().list().setQ("trashed = false and '" + parentId + "' in parents").setMaxResults(100);
				do {
					FileList result = request.execute();
					request.setPageToken(result.getNextPageToken());
					
					for (File remoteFile : result.getItems()) {
						String title = Main.encodeFilename(remoteFile.getTitle());
						if (title == null || title.isEmpty()) continue;
						
						String mimeType = remoteFile.getMimeType();
						if (mimeType != null && mimeType.equals("application/vnd.google-apps.folder")) {
							Path localPath = parentPath.resolve(title);
							if (!Files.isDirectory(localPath)) {
								if (Files.exists(localPath)) {
									System.out.println("Conflict: local file exists and is not directory");
									continue;
								}
								
								Files.createDirectories(localPath);
							}
							ListRemoteFilesTask subtask = new ListRemoteFilesTask(remoteFile.getId(), localPath);
							listRemoteFilesService.execute(subtask);
						}
						else {
							if (remoteFile.getFileSize() == null) {
								System.out.println("Remote file " + remoteFile.getTitle() + " has no data, skipping");
								continue;
							}
							
							LocalFileResolution localFile = resolveLocalFile(remoteFile, title);
							
							if (Config.pullComputeMd5 && localFile.exists && (localFile.metadata == null || localFile.metadata.getMd5() == null)) {
								CheckMd5Task subtask = new CheckMd5Task(remoteFile, localFile.path);
								checkMd5Service.execute(subtask);
								continue;
							}
							
							if (localFile.metadata != null && localFile.metadata.getMd5() != null && remoteFile.getMd5Checksum() != null && localFile.metadata.getMd5().equals(remoteFile.getMd5Checksum())) {
								long localSize = Files.size(localFile.path);
								long localTime = Files.getLastModifiedTime(localFile.path).toMillis();
								
								if (localSize == remoteFile.getFileSize()) {
									if (remoteFile.getModifiedDate().getValue() == localTime) {
										System.out.println("ID, MD5, size, and LastModifiedTime match, skipping download of " + localFile.path);
										continue;
									}
									if (Config.pullComputeMd5) {
										System.out.println("ID, MD5, size match, but LastModifiedTime does not match, checking local MD5 of " + localFile.path);
										CheckMd5Task subtask = new CheckMd5Task(remoteFile, localFile.path);
										checkMd5Service.execute(subtask);
										continue;
									}
								}
							}
							
							DownloadFileTask subtask = new DownloadFileTask(remoteFile, localFile.path);
							downloadService.execute(subtask);
						}
					}
				} while (request.getPageToken() != null && !request.getPageToken().isEmpty());
			}
			catch (Exception e) {
				System.out.println("Error in ListRemoteFilesTask");
				System.out.println(e.getMessage());
			}
		}
	}
	
	/* Check the md5 of a local file against a remote file. If they match, we're done processing.
	 * If they don't match, we need to download the remote file.
	 */
	private class CheckMd5Task implements Runnable {
		private File remoteFile;
		private Path localPath;
		private ByteBuffer byteBuffer = ByteBuffer.allocate(16 * 1024);
		
		public CheckMd5Task(File remoteFile, Path localPath) {
			this.remoteFile = remoteFile;
			this.localPath = localPath;
		}
		
		public void run() {
			System.out.println("Computing MD5 of " + localPath);
			try {
				MessageDigest md = MessageDigest.getInstance("MD5");
				ReadableByteChannel in = Files.newByteChannel(localPath);
				byteBuffer.clear();
				while (in.read(byteBuffer) != -1) {
					byteBuffer.flip();
					md.update(byteBuffer);
					byteBuffer.clear();
				}
				String fileMd5 = new String(Hex.encodeHex(md.digest()));
				if (fileMd5.equalsIgnoreCase(remoteFile.getMd5Checksum())) {
					System.out.println("MD5 matches, adding metadata");
					/* We have the file locally without the metadata. Just add the metadata. */
					JavaDriveToolMetaDataV1 metadata = new JavaDriveToolMetaDataV1();
					metadata.setFileId(remoteFile.getId());
					metadata.setMd5(remoteFile.getMd5Checksum());
					Gson gson = new Gson();
					String data = gson.toJson(metadata);
					UserDefinedFileAttributeView view = Files.getFileAttributeView(localPath, UserDefinedFileAttributeView.class);
					view.write("javadrivetool", StandardCharsets.UTF_8.encode(data));
					
					/* Set timestamp. */
					Files.setLastModifiedTime(localPath, FileTime.fromMillis(remoteFile.getModifiedDate().getValue()));
				}
				else {
					/* MD5 does not match, download the file. */
					DownloadFileTask subtask = new DownloadFileTask(remoteFile, localPath);
					downloadService.execute(subtask);
				}
			}
			catch (Exception e) {
				System.out.println("Error in CheckMd5Task: " + e.getMessage());
			}
		}
	}
	
	private class DownloadFileTask implements Runnable {
		private File remoteFile;
		private Path localPath;
		
		public DownloadFileTask(File remoteFile, Path localPath) {
			this.remoteFile = remoteFile;
			this.localPath = localPath;
		}
		
		public void run() {
			System.out.println("Downloading " + localPath);
			OutputStream outputStream = null;
			try {
				outputStream = Files.newOutputStream(localPath);
				
				/* Download file. */
				drive.files().get(remoteFile.getId()).executeMediaAndDownloadTo(outputStream);
				
				outputStream.close();
				
				/* Save metadata. */
				JavaDriveToolMetaDataV1 metadata = new JavaDriveToolMetaDataV1();
				metadata.setFileId(remoteFile.getId());
				metadata.setMd5(remoteFile.getMd5Checksum());
				Gson gson = new Gson();
				String data = gson.toJson(metadata);
				UserDefinedFileAttributeView view = Files.getFileAttributeView(localPath, UserDefinedFileAttributeView.class);
				view.write("javadrivetool", StandardCharsets.UTF_8.encode(data));
				
				/* Set timestamp. */
				Files.setLastModifiedTime(localPath, FileTime.fromMillis(remoteFile.getModifiedDate().getValue()));
			}
			catch (Exception e) {
				System.out.println("Error in DownloadFileTask");
				System.out.println(e.getMessage());
				if (outputStream != null) {
					try {
						outputStream.close();
					}
					catch (Exception f) {
					}
				}
			}
			System.out.println("Done downloading " + localPath);
		}
	}
	
	public PullService(Drive drive, Path localRoot) {
		this.drive = drive;
		this.localRoot = localRoot;
		listRemoteFilesService = Executors.newFixedThreadPool(1);
		if (Config.pullComputeMd5) checkMd5Service = Executors.newFixedThreadPool(2);
		downloadService = Executors.newFixedThreadPool(4);
	}
	
	public void pull() {
		ListRemoteFilesTask mainTask = new ListRemoteFilesTask("root", localRoot);
		listRemoteFilesService.execute(mainTask);
	}
}
