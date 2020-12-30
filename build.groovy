import org.freecompany.redline.Builder
import org.freecompany.redline.header.Architecture
import org.freecompany.redline.header.Os
import org.freecompany.redline.header.RpmType
import org.freecompany.redline.payload.Directive
import groovy.yaml.*
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

//Do not use '-' in version string, it breaks rpm uninstall.
release = saw.getProperty("KAIROS_RELEASE_NUMBER", "1") //package release number

saw = Tablesaw.getCurrentTablesaw()
saw.includeDefinitionFile("definitions.xml")


rpmDir = "target/rpm"
new DirectoryRule("target")
rpmDirRule = new DirectoryRule(rpmDir)

//Read pom file to get version out
def pom = new XmlSlurper().parseText(new File("pom.xml").text)
version = pom.version.text()

//Read stork definitions
storkYml = new YamlSlurper().parse(new File("src/main/launchers/stork.yml"))
programName = storkYml.name
summary = storkYml.short_description
description = storkYml.long_description


mvnRule = new SimpleRule("maven-package")
		.setDescription("Run maven package build")
		.setMakeAction("doMavenBuild")

def doMavenBuild(Rule rule)
{
	saw.exec("mvn package")
}

rpmFile = "$programName-$version-${release}.rpm"
srcRpmFile = "$programName-$version-${release}.src.rpm"

//------------------------------------------------------------------------------
//Build rpm file
rpmBaseInstallDir = "/opt/topic-monitor"
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

	rpmBuilder.setPrefixes(rpmBaseInstallDir)
	rpmBuilder.addDependencyMore("jre", storkYml.min_java_version)

	rpmBuilder.addFile("$rpmBaseInstallDir/bin/topic-monitor", new File("target/stork/bin/topic-monitor"), 0755);

	rpmBuilder.addFile("$rpmBaseInstallDir/conf/application.conf",
			new File("target/stork/conf/application.conf"), 0644, new Directive(Directive.RPMFILE_CONFIG | Directive.RPMFILE_NOREPLACE));
	rpmBuilder.addFile("$rpmBaseInstallDir/conf/logback.xml",
			new File("target/stork/conf/logback.xml"), 0644, new Directive(Directive.RPMFILE_CONFIG | Directive.RPMFILE_NOREPLACE));
	rpmBuilder.addFile("$rpmBaseInstallDir/conf/metrics4j.conf",
			new File("target/stork/conf/metrics4j.conf"), 0644, new Directive(Directive.RPMFILE_CONFIG | Directive.RPMFILE_NOREPLACE));

	addFileSetToRPM(rpmBuilder, "$rpmBaseInstallDir/lib", new RegExFileSet("target/stork/lib", ".*\\.jar"))
	addFileSetToRPM(rpmBuilder, "$rpmBaseInstallDir/share", new RegExFileSet("target/stork/share", ".*").recurse())

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
