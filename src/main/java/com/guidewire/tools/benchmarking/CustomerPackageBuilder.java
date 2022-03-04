package com.guidewire.tools.benchmarking;

import com.guidewire.tools.DataExtractionUtils;
import com.guidewire.tools.MiscUtils;
import com.guidewire.tools.PropertiesUtils;
import org.apache.commons.cli.*;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.*;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.*;


public class CustomerPackageBuilder {

  private static final String CONFIG_TEMPLATE_FILE = "config_template.xml";
  private static final String CUSTOMER_BUILD_DIR = "customerBuilds/";

  private static final int BUFFER = 2048;
  private Properties properties;

  private void packageApplication(CustomerConfig config) throws IOException {
    debug("Creating config for customer [" + config.getCode() + "]");
    createDirs(config);
    debug("copying files");
    copyFiles(config);
    debug("writing config file");
    writeConfigFile(config);
    debug("compressing results");
    compressResults(config);
  }


  private InputStream getStreamFor(String fileName) throws FileNotFoundException {
    for (String dir : new String[]{"config"}) {
      File fullPath = new File(dir, fileName);
      if (fullPath.exists()) return new FileInputStream(fullPath);
    }
    return getClass().getClassLoader().getResourceAsStream(fileName);
  }
  
  private void writeConfigFile(CustomerConfig config) throws IOException {
    BufferedReader template = getConfigTemplateReader();
    BufferedWriter output = getConfigFileWriter(config);
    Properties substitutions = new Properties();

    String row;
    while ((row = template.readLine()) != null) {
      String outputRow = MiscUtils.transform(row, substitutions);
      output.write(outputRow + "\n");
    }
    output.close();
    template.close();
  }

  private BufferedWriter getConfigFileWriter(CustomerConfig config) throws IOException {
    File dir = new File(getBuildDirectory(config));
    debug("Checking dirs: " + dir.getAbsolutePath() + " " + dir.mkdirs());
    File file = new File(dir, "config.xml");
    debug("creating: " + file.getAbsolutePath());
    return new BufferedWriter(new FileWriter(file));
  }

  private String getBuildDirectory(CustomerConfig config) {
    return CUSTOMER_BUILD_DIR + config.getCode();
  }

  private BufferedReader getConfigTemplateReader() throws FileNotFoundException {
    return new BufferedReader(new InputStreamReader(getStreamFor(CONFIG_TEMPLATE_FILE)));
  }

  private void createDirs(CustomerConfig config) {
    String code = config.getCode();
    File root = new File(CUSTOMER_BUILD_DIR + code);
    for (String dirName : dirsToCreate()) {
      File dir = new File(root, dirName);
      debug("Creating dir: " + dir.getAbsolutePath());
      if (!dir.mkdirs()) {
        throw new RuntimeException("Couldn't create directory: " + dir.getPath());
      }
    }
  }

  private void deleteCustomerDirectory(CustomerConfig config) {
    File root = new File(CUSTOMER_BUILD_DIR + config.getCode());
    deleteDirectoryTree(root);
  }

  private void deleteDirectoryTree(File dir) {
    for (File file : dir.listFiles()) {
      if (file.isDirectory()) {
        deleteDirectoryTree(file);
        if (!file.delete()) {
          throw new RuntimeException("Couldn't delete file: " + file.getPath());
        }
      } else {
        if (!file.delete()) {
          throw new RuntimeException("Couldn't delete file: " + file.getPath());
        }
      }
    }
    // And delete the parent folder as well.
    if (!dir.delete()) {
      throw new RuntimeException("Couldn't delete dir: " + dir.getPath());
    }
  }

  private String[] dirsToCreate() {
    return new String[]{"bin", "lib", "working", "logs", "config", "src"};
  }

  private void copyFiles(CustomerConfig config) throws FileNotFoundException {
    String jarFile = MiscUtils.transform("dataExtraction-guidewire.extractor.version-jar-with-dependencies.jar", properties);
    File fromJar = new File("target", jarFile);
    String customerHome = CUSTOMER_BUILD_DIR + config.getCode();
    File toJar = new File(customerHome + "/lib", jarFile);
    copyFile(fromJar, toJar);

    for (String batFile : new String[]{"extract.sh", "extract.bat", "full_extract.sh", "full_extract.bat", "incremental_update.sh", "incremental_update.bat", "test_connection.sh", "test_connection.bat"}) {
      File fromBat = new File("target/classes/resources", batFile);
      File toBat = new File(customerHome + "/bin", batFile);
      copyFile(fromBat, toBat);
    }

    File versionFile = new File("target/classes", "extract.properties");
    copyFile(versionFile, new File(customerHome + "/config", versionFile.getName()));
    copyDirectoryTree("src", customerHome + "/src");
    copyFile(getStreamFor("logsDirectory.txt"), new File(customerHome + "/logs/logsDirectory.txt"));
  }

  private void copyDirectoryTree(String src, String dest) {
    copyDirectoryTree(new File(src), dest);
  }

  private void copyDirectoryTree(File src, String dest) {
    if (src == null)
      throw new IllegalArgumentException("Null src file.  Destination is [" + dest + "]");
    for (File fromFile : src.listFiles()) {
      File toFile = new File(dest, fromFile.getName());
      if (fromFile.isDirectory()) {
        toFile.mkdirs();
        copyDirectoryTree(fromFile, dest + File.separator + fromFile.getName());
      } else {
        copyFile(fromFile, toFile);
      }
    }
  }

  private void useProperties(Properties props) {
    properties = props;
  }

  private void debug(String s) {
    System.out.println(s);
  }

  private static void copyFile(File fromFile, File toFile) {
    try {
      copyFile(new FileInputStream(fromFile), toFile);
    } catch (FileNotFoundException ex) {
      System.out.println(ex.getMessage() + " in the specified directory.");
      System.exit(-1);
    }
  }

  private static void copyFile(InputStream in, File toFile) {
    String fileName = toFile.getName();
    String path = toFile.getPath();
    if (fileName.endsWith(".sh")) {
      toFile.setExecutable(true);
    }
    if (in == null) return;
    try {
      //For Overwrite the file.
      OutputStream out = new FileOutputStream(toFile);
      byte[] buf = new byte[1024];
      int len;
      while ((len = in.read(buf)) > 0) {
        out.write(buf, 0, len);
      }
      in.close();
      out.close();
    } catch (IOException e) {
      System.out.println(e.getMessage());
    }
  }

  private void compressResults(CustomerConfig config) throws IOException {
    // Create a .zip file
    String filename = CUSTOMER_BUILD_DIR + config.getCode() + ".zip";
    File customerDir = new File(CUSTOMER_BUILD_DIR, config.getCode());
    customerDir.listFiles();
    FileOutputStream dest = new FileOutputStream(filename);
    CheckedOutputStream checksum = new CheckedOutputStream(dest, new Adler32());
    ZipOutputStream out = new ZipOutputStream(new BufferedOutputStream(checksum));
    for (File file : customerDir.listFiles()) {
      compressTo(file, config.getCode(), out);
    }
    out.flush();
    out.close();

    // create a tar.gz file for unix installations
    String tarGzFilename = CUSTOMER_BUILD_DIR + config.getCode() + ".tar.gz";
    dest = new FileOutputStream(tarGzFilename);
    BufferedOutputStream bos = new BufferedOutputStream(dest);
    GZIPOutputStream gzos = new GZIPOutputStream(bos);
    TarArchiveOutputStream taos = new TarArchiveOutputStream(gzos);
    for (File file : customerDir.listFiles()) {
      compressToTarGz(file, config.getCode() + "/", taos);
    }
    taos.flush();
    taos.close();
  }

  private void compressTo(File file, String relativePath, ZipOutputStream out) throws IOException {
    if (file.isDirectory()) {
      for (File subFile : file.listFiles()) {
        compressTo(subFile, relativePath + File.separator + file.getName(), out);
      }
      return;
    }
    String zippedFile = relativePath + File.separator + file.getName();
//    debug("Zipping: " + zippedFile);
    out.putNextEntry(new ZipEntry(zippedFile));
    BufferedInputStream input = new BufferedInputStream(new FileInputStream(file), BUFFER);
    int length;
    byte data[] = new byte[BUFFER];
    while ((length = input.read(data, 0, BUFFER)) > 0) {
      out.write(data, 0, length);
    }
    out.closeEntry();
    input.close();
  }

  private void compressToTarGz(File file, String base, TarArchiveOutputStream out) throws IOException {
    if (file.getName().endsWith(".sh")) {
      file.setExecutable(true);
    }
    TarArchiveEntry tarEntry = new TarArchiveEntry(file, base + file.getName());
    if (file.getName().endsWith(".sh")) {
      // Force the mode for scripts and executables to have execute bit set.
      int mode = tarEntry.getMode();
      int exeMode = mode | 00100;
      tarEntry.setMode(exeMode);
    }
    out.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
    out.putArchiveEntry(tarEntry);
//    String zippedFile = tarEntry.getName();
//    debug("tarring: " + zippedFile);

    if (file.isDirectory()) {
      out.closeArchiveEntry();
      for (File subFile : file.listFiles()) {
        compressToTarGz(subFile, base + file.getName() + "/", out);
      }
    } else {
      IOUtils.copy(new FileInputStream(file), out);
      out.closeArchiveEntry();
    }
  }

  private static String removeWhitespace(String input) {
    Pattern whitespace = Pattern.compile("\\s");
    Matcher matcher = whitespace.matcher(input);
    return matcher.replaceAll("");
  }

  private void setDefaultVersionInformation() throws IOException {
    useProperties(PropertiesUtils.getProperties(DataExtractionUtils.getInputStreamBasedOnFileName("extract.properties")));
  }

  private static String DefaultConfigFile = "config.xml";
  private static final String HELP = "help";
  private static final String FILE = "file";
  private static final String BUILD_ALL = "buildall";

  private static Options getCommandLineOptions() {
    Options options = new Options();
    options.addOption("b", BUILD_ALL, false, "create the query configuration based on the file provided");
    options.addOption("h", HELP, false, "display this message");
    options.addOption("f", FILE, true, "specify the config file to use.  Defaults to config.xml");
    return options;
  }

  private static CustomerPackageBuilder createProductionPackageBuilder() throws IOException, CustomerConfig.InvalidDatabaseType {
    CustomerPackageBuilder packageBuilder = new CustomerPackageBuilder();
    packageBuilder.setDefaultVersionInformation();
    return packageBuilder;
  }

  public static void main(String[] args) throws Exception {
//      CustomerConfig config = new CustomerConfig();
//      config.setCode("extractor");
//      config.setName("LV");
//      config.setVersion("cc6");
//      config.setDB("oracle");
//    createProductionPackageBuilder().packageApplication(config);

    int i = 0;
    while (i < args.length) {
      args[i] = args[i].toLowerCase();
      i++;
    }

    Options allOptions = getCommandLineOptions();
    // This option is only added after help has been displayed  This is not a mode of
    // running we can expose
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(allOptions, args);

    if (cmd.hasOption(HELP)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("benchmarking", getCommandLineOptions());
      return;
    }

    if (cmd.hasOption(BUILD_ALL)) {
      CustomerConfig config = new CustomerConfig();
      config.setCode("extractor");
      config.setName("extractor");
      config.setVersion("cc");
      config.setDB("DBMS");
      createProductionPackageBuilder().packageApplication(config);
    }
  }
}