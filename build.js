importPackage(Packages.tablesaw);
importPackage(Packages.tablesaw.rules);
importPackage(Packages.org.freecompany.redline);
importPackage(Packages.org.freecompany.redline.header);
importPackage(Packages.org.freecompany.redline.payload);
importPackage(Packages.groovy.yaml);
importPackage(Packages.java.nio.file);
importPackage(Packages.java.io);
importPackage(Packages.org.yaml.snakeyaml);
importPackage(Packages.javax.swing);

saw.setProperty(Tablesaw.PROP_MULTI_THREAD_OUTPUT, Tablesaw.PROP_VALUE_ON);

//Do not use '-' in version string, it breaks rpm uninstall.
var release = saw.getProperty("KAIROS_RELEASE_NUMBER", "1"); //package release number

saw.includeDefinitionFile("definitions.xml");

var rpmDir = "target/rpm";
new DirectoryRule("target");
var rpmDirRule = new DirectoryRule(rpmDir);

//Read pom file to get version out
var pom = saw.parsePomFile("pom.xml");
var version = pom.getVersion();

//Read stork definitions
var yaml = new Yaml();
var yamlMap = yaml.load(Files.readString(Path.of("src/main/launchers/stork.yml")));

var programName = yamlMap.get("name");
var summary = yamlMap.get("short_description");
var description = yamlMap.get("long_description");


var mvnRule = new SimpleRule("maven-package")
		.setDescription("Run maven package build")
		.setMakeAction("doMavenBuild");

function doMavenBuild(rule)
{
	saw.exec(`mvn package`);
}

rpmFile = `${programName}-${version}-${release}.rpm`
srcRpmFile = `${programName}-${version}-${release}.src.rpm`

//------------------------------------------------------------------------------
//Build rpm file
rpmBaseInstallDir = "/opt/topic-monitor"
rpmRule = new SimpleRule("package-rpm").setDescription("Build RPM Package")
		.addDepend(mvnRule)
		.addDepend(rpmDirRule)
		.addTarget(`${rpmDir}/${rpmFile}`)
		.setMakeAction("doRPM")
		.setProperty("dependency", "on");

function doRPM(rule)
{
	//Build rpm using redline rpm library
	host = Packages.java.net.InetAddress.getLocalHost().getHostName();
	rpmBuilder = new Builder();
	rpmBuilder.setDescription(description);
	rpmBuilder.group = "System Environment/Daemons";
	rpmBuilder.license = "license";
	rpmBuilder.setPackage(programName, version, release);
	rpmBuilder.setPlatform(Architecture.NOARCH, Os.LINUX);
	rpmBuilder.summary = summary;
	rpmBuilder.type = RpmType.BINARY;
	rpmBuilder.url = "http://kairosdb.org";
	rpmBuilder.vendor = "KairosDB";
	rpmBuilder.provides = programName;
				//prefixes = rpmBaseInstallDir
	rpmBuilder.buildHost = host;
	rpmBuilder.sourceRpm = srcRpmFile;

	rpmBuilder.setPrefixes(rpmBaseInstallDir);
	rpmBuilder.addDependencyMore("jre", yamlMap.get("min_java_version"));

	rpmBuilder.addFile(`${rpmBaseInstallDir}/bin/topic-monitor`, new File("target/stork/bin/topic-monitor"), 0755);

	rpmBuilder.addFile(`${rpmBaseInstallDir}/conf/application.conf`,
			new File("target/stork/conf/application.conf"), 0644, new Directive(Directive.RPMFILE_CONFIG | Directive.RPMFILE_NOREPLACE));
	rpmBuilder.addFile(`${rpmBaseInstallDir}/conf/logback.xml`,
			new File("target/stork/conf/logback.xml"), 0644, new Directive(Directive.RPMFILE_CONFIG | Directive.RPMFILE_NOREPLACE));
	rpmBuilder.addFile(`${rpmBaseInstallDir}/conf/metrics4j.conf`,
			new File("target/stork/conf/metrics4j.conf"), 0644, new Directive(Directive.RPMFILE_CONFIG | Directive.RPMFILE_NOREPLACE));

	addFileSetToRPM(rpmBuilder, `${rpmBaseInstallDir}/lib`, new RegExFileSet("target/stork/lib", ".*\\.jar"));
	addFileSetToRPM(rpmBuilder, `${rpmBaseInstallDir}/share`, new RegExFileSet("target/stork/share", ".*").recurse());

	print(`Building RPM ${rule.getTarget()}`);
	outputFile = new FileOutputStream(rule.getTarget());
	rpmBuilder.build(outputFile.channel);
	outputFile.close();
}

function addFileSetToRPM(builder, destination, files)
{
	files.getFiles().forEach(function(file)
	{
		var f = new File(file.getBaseDir(), file.getFile());
		builder.addFile(destination + "/" + file.getFile(), f);
	});
}


debRule = new SimpleRule("package-deb").setDescription("Build Deb Package")
		.addDepend(rpmRule)
		.setMakeAction("doDeb");

function doDeb(rule)
{
	//Prompt the user for the sudo password
	//TODO: package using jdeb
	var jpf = new JPasswordField();
	var password = saw.getProperty("sudo");

	if (password == null)
	{
		var resp = JOptionPane.showConfirmDialog(null,
				jpf, "Enter sudo password:",
				JOptionPane.OK_CANCEL_OPTION);

		if (resp == 0)
			password = jpf.getText();
	}

	if (password != null)
	{
		var sudo = saw.createAsyncProcess(rpmDir, `sudo -S alien --scripts --bump=0 --to-deb ${rpmFile}`);
		sudo.run();
		//pass the password to the process on stdin
		sudo.sendMessage(password+"\n");
		sudo.waitForProcess();
		if (sudo.getExitCode() != 0)
			throw new TablesawException("Unable to run alien application");
	}
}



//------------------------------------------------------------------------------
//Build notification
function printMessage(strTitle, strMessage)
{
	osName = saw.getProperty("os.name");

	var notifyDef;
	if (osName.startsWith("Linux"))
	{
		notifyDef = saw.getDefinition("linux-notify");
	}
	else if (osName.startsWith("Mac"))
	{
		notifyDef = saw.getDefinition("mac-notify");
	}

	if (notifyDef != null)
	{
		notifyDef.set("title", strTitle);
		notifyDef.set("message", strMessage);
		saw.exec(notifyDef.getCommand());
	}
}

function buildFailure(exception)
{
	printMessage("Build Failure", exception.getMessage())
}

function buildSuccess(target)
{
	printMessage("Build Success", target)
}