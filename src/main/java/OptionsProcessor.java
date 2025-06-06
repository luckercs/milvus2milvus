import org.apache.commons.cli.*;

public class OptionsProcessor {
    private final Options argOptions = new Options();
    private CommandLine commandLine;
    private static final String project = "milvus2milvus";

    public OptionsProcessor() {
        argOptions.addOption(new Option("u", "uri", true, "milvus uri address, eg: http://localhost:19530"));
        argOptions.addOption(new Option("t", "token", true, "milvus token, eg: root:Milvus"));
        argOptions.addOption(new Option("c", "collections", true, "milvus collection, eg: hello_milvus"));
        argOptions.addOption(new Option("s", "skip", true, "milvus skip collection, eg: hello_milvus"));
        argOptions.addOption(new Option("ss", "skip_schema", false, "milvus skip collection schema create"));
        argOptions.addOption(new Option("si", "skip_index", false, "milvus skip collection index create"));
        argOptions.addOption(new Option("tu", "t_uri", true, "target milvus uri address, eg: http://localhost:19530"));
        argOptions.addOption(new Option("tt", "t_token", true, "target milvus token, eg: root:Milvus"));
        argOptions.addOption(new Option("b", "batchsize", true, "milvus read and write batchsize, eg: 1000"));
        argOptions.addOption(new Option("h", "help", true, "show help"));
    }

    public CommandLine parse(String[] args) {
        try {
            commandLine = new GnuParser().parse(argOptions, args);
            if (commandLine.hasOption("h")) {
                printUsage();
                System.exit(0);
            }
        } catch (ParseException e) {
            System.err.println("parse cmd err: " + e.getMessage());
            printUsage();
            System.exit(1);
        }
        return commandLine;
    }

    public void printUsage() {
        new HelpFormatter().printHelp(project, argOptions);
    }
}
