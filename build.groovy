import org.freecompany.redline.Builder
import org.freecompany.redline.header.Architecture
import org.freecompany.redline.header.Os
import org.freecompany.redline.header.RpmType
import org.freecompany.redline.payload.Directive
import tablesaw.*
import tablesaw.addons.GZipRule
import tablesaw.addons.TarRule
import tablesaw.addons.ivy.IvyAddon
import tablesaw.addons.ivy.PomRule
import tablesaw.addons.ivy.PublishRule
import tablesaw.addons.java.Classpath
import tablesaw.addons.java.JarRule
import tablesaw.addons.java.JavaCRule
import tablesaw.addons.java.JavaProgram
import tablesaw.addons.junit.JUnitRule
import tablesaw.definitions.Definition
import tablesaw.rules.CopyRule
import tablesaw.rules.DirectoryRule
import tablesaw.rules.Rule
import tablesaw.rules.SimpleRule

import javax.swing.*

println("===============================================")

saw.setProperty(Tablesaw.PROP_MULTI_THREAD_OUTPUT, Tablesaw.PROP_VALUE_ON)

programName = "kairos-kafka-monitor"
//Do not use '-' in version string, it breaks rpm uninstall.
version = "1.3.0"
release = saw.getProperty("KAIROS_RELEASE_NUMBER", "0.1beta") //package release number
summary = "KairosDB"
description = """\
KairosDB is a time series database that stores numeric values along
with key/value tags to a nosql data store.  Currently supported
backends are Cassandra and H2.  An H2 implementation is provided
for development work.
"""


saw = Tablesaw.getCurrentTablesaw()
saw.includeDefinitionFile("definitions.xml")


rpmDir = "build/rpm"
new DirectoryRule("build")
rpmDirRule = new DirectoryRule(rpmDir)

//Read pom file to get version out
def pom = new XmlSlurper().parseText(new File("pom.xml").text)
version = pom.version.text()

mvnRule = new SimpleRule("maven-package")
		.setDescription("Run maven package build")
		.setMakeAction("doMavenBuild")

def doMavenBuild(Rule rule)
{
	saw.exec("mvn clean package")
}

//------------------------------------------------------------------------------
//Build zip deployable application
rpmFile = "$programName-$version-${release}.rpm"
srcRpmFile = "$programName-$version-${release}.src.rpm"
ivyFileSet = new SimpleFileSet()


libFileSets = [
		new RegExFileSet("build/jar", ".*\\.jar"),
		new RegExFileSet("lib", ".*\\.jar"),
		ivyFileSet
]

scriptsFileSet = new RegExFileSet("src/scripts", ".*").addExcludeFile("kairosdb-env.sh")
webrootFileSet = new RegExFileSet("webroot", ".*").recurse()

zipLibDir = "$programName/lib"
zipBinDir = "$programName/bin"
zipConfDir = "$programName/conf"
zipConfLoggingDir = "$zipConfDir/logging"
zipWebRootDir = "$programName/webroot"
tarRule = new TarRule("build/${programName}-${version}-${release}.tar")
		.addDepend(mvnRule)
		.addFileSetTo(zipBinDir, scriptsFileSet)
		.addFileSetTo(zipWebRootDir, webrootFileSet)
		.addFileTo(zipConfDir, "src/main/resources", "kairosdb.conf")
		.addFileTo(zipConfLoggingDir, "src/main/resources", "logback.xml")
		.setFilePermission(".*\\.sh", 0755)

for (AbstractFileSet fs in libFileSets)
	tarRule.addFileSetTo(zipLibDir, fs)


gzipRule = new GZipRule("package").setSource(tarRule.getTarget())
		.setDescription("Create deployable tar file")
		.setTarget("build/${programName}-${version}-${release}.tar.gz")
		.addDepend(tarRule)

//------------------------------------------------------------------------------
//Build rpm file
rpmBaseInstallDir = "/opt/kairosdb"
rpmRule = new SimpleRule("package-rpm").setDescription("Build RPM Package")
		.addDepend(mvnRule)
		.addDepend(rpmDirRule)
		.addTarget("$rpmDir/$rpmFile")
		.setMakeAction("doRPM")
		.setProperty("dependency", "on")


def doRPM(Rule rule)
{
	//Build rpm using redline rpm library
	host = InetAddress.getLocalHost().getHostName()
	rpmBuilder = new Builder()
	rpmBuilder.with
			{
				description = description
				group = "System Environment/Daemons"
				license = "license"
				setPackage(programName, version, release)
				setPlatform(Architecture.NOARCH, Os.LINUX)
				summary = summary
				type = RpmType.BINARY
				url = "http://kairosdb.org"
				vendor = "KairosDB"
				provides = programName
				//prefixes = rpmBaseInstallDir
				buildHost = host
				sourceRpm = srcRpmFile
			}

	rpmBuilder.addDependencyMore("kairosdb", "1.2.0")

	addFileSetToRPM(rpmBuilder, "$rpmBaseInstallDir/lib/kafka-monitor", new RegExFileSet("target", ".*\\.jar"))
	addFileSetToRPM(rpmBuilder, "$rpmBaseInstallDir/lib/kafka-monitor", new RegExFileSet("target/dependency", ".*\\.jar"))


	rpmBuilder.addFile("$rpmBaseInstallDir/conf/kafka-monitor.properties",
			new File("src/main/resources/kafka-monitor.properties"), 0644, new Directive(Directive.RPMFILE_CONFIG | Directive.RPMFILE_NOREPLACE))


	println("Building RPM "+rule.getTarget())
	outputFile = new FileOutputStream(rule.getTarget())
	rpmBuilder.build(outputFile.channel)
	outputFile.close()
}

def addFileSetToRPM(Builder builder, String destination, AbstractFileSet files)
{

	for (AbstractFileSet.File file : files.getFiles())
	{
		File f = new File(file.getBaseDir(), file.getFile())
		if (f.getName().endsWith(".sh"))
			builder.addFile(destination + "/" +file.getFile(), f, 0755)
		else
			builder.addFile(destination + "/" + file.getFile(), f)
	}
}

debRule = new SimpleRule("package-deb").setDescription("Build Deb Package")
		.addDepend(rpmRule)
		.setMakeAction("doDeb")

def doDeb(Rule rule)
{
	//Prompt the user for the sudo password
	//TODO: package using jdeb
	def jpf = new JPasswordField()
	def password = saw.getProperty("sudo")

	if (password == null)
	{
		def resp = JOptionPane.showConfirmDialog(null,
				jpf, "Enter sudo password:",
				JOptionPane.OK_CANCEL_OPTION)

		if (resp == 0)
			password = jpf.getPassword()
	}

	if (password != null)
	{
		sudo = saw.createAsyncProcess(rpmDir, "sudo -S alien --bump=0 --to-deb $rpmFile")
		sudo.run()
		//pass the password to the process on stdin
		sudo.sendMessage("$password\n")
		sudo.waitForProcess()
		if (sudo.getExitCode() != 0)
			throw new TablesawException("Unable to run alien application")
	}
}



//------------------------------------------------------------------------------
//Build notification
def printMessage(String title, String message) {
	osName = saw.getProperty("os.name")

	Definition notifyDef
	if (osName.startsWith("Linux"))
	{
		notifyDef = saw.getDefinition("linux-notify")
	}
	else if (osName.startsWith("Mac"))
	{
		notifyDef = saw.getDefinition("mac-notify")
	}

	if (notifyDef != null)
	{
		notifyDef.set("title", title)
		notifyDef.set("message", message)
		saw.exec(notifyDef.getCommand())
	}
}

def buildFailure(Exception e)
{
	printMessage("Build Failure", e.getMessage())
}

def buildSuccess(String target)
{
	printMessage("Build Success", target)
}
