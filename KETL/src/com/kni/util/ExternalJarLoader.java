/* Copyright (C) 2008 Kinetic Networks Inc. All rights reserved.
 */
package com.kni.util;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * @author Srinivas Jaini
 * @version Mar 27, 2008
 */
public class ExternalJarLoader {
	private static final Logger logger = Logger
			.getLogger(ExternalJarLoader.class.getName());
	private static final Class[] parameters = new Class[] { URL.class };

	public static void addFile(String s) throws IOException {
		File f = new File(s);
		addFile(f);
	}

	public static void addFile(File f) throws IOException {
		addURL(f.toURL());
	}

	public static void addURL(URL... u) throws IOException {

		URLClassLoader urlClassLoader = (URLClassLoader) ClassLoader
				.getSystemClassLoader();
		Class urlClassLoaderClass = URLClassLoader.class;

		try {
			Method method = urlClassLoaderClass.getDeclaredMethod("addURL",
					parameters);
			method.setAccessible(true);
			method.invoke(urlClassLoader, u);
			for (URL url : u) {
				logger.fine("Added to classpath: " + url.getFile());
			}
		} catch (Throwable t) {
			t.printStackTrace();
			throw new IOException(
					"Error, could not add URL to system classloader");
		}

	}

	public static URL[] readExtraLibrariesFromPropertyFile(String fileName,
			String propertyName, String separator) {
		Properties properties = new Properties();
		try {
			properties.load(ExternalJarLoader.class.getClassLoader()
					.getResourceAsStream(fileName));
			String filePath = properties.getProperty(propertyName);
			String[] paths = filePath.split(separator);
			List<URL> urls = new ArrayList<URL>();
			for (String path : paths) {
				File file = new File(path);
				File[] jarFiles = browseJarFiles(file);
				if (jarFiles != null) {
					for (File jarFile : jarFiles) {
						urls.add(jarFile.getAbsoluteFile().toURL());
					}
				}

			}
			return urls.toArray(new URL[urls.size()]);
		} catch (IOException e) {
			logger.severe(e.getMessage());
			throw new RuntimeException("Couldnot load jar files into classpath");
		}

	}

	/**
	 * @param file
	 * @return
	 */
	private static File[] browseJarFiles(File file) {
		File[] jarFiles = file.listFiles(new FilenameFilter() {
			public boolean accept(File arg0, String name) {
				if (name.endsWith(".jar"))
					return true;
				else
					return false;
			}

		});
		return jarFiles;
	}

	public static void main(String[] args) {
		// loadJars();
		// testAClass();
	}

	/**
	 * 
	 */
	public static void loadJars(String fileName, String propertyName,
			String separator) {
		try {
			loadToolsJarFromJavaHome();
			loadJarsFromLibDirectory();
			addJarFilesToClassPath(readExtraLibrariesFromPropertyFile(fileName,
					propertyName, separator));
		} catch (MalformedURLException e) {
			logger.severe(e.getMessage());
		} catch (IOException e) {
			logger.severe(e.getMessage());
		}
	}

	/**
	 * @throws IOException
	 * 
	 */
	private static void addJarFilesToClassPath(URL... urls) throws IOException {
		for (URL url : urls) {
			addURL(url);
		}
	}

	private static void loadToolsJarFromJavaHome() throws IOException {
		File javaHome = new File(System.getProperty("java.home")
				+ File.separator + "lib" + File.separator + "tools.jar");
		logger.fine("loading... tools.jar");
		addJarFilesToClassPath(javaHome.toURL());

	}

	private static void loadJarsFromLibDirectory() throws IOException {
		File ketlHome = new File(System.getProperty("user.dir")
				+ File.separator + "lib" + File.separator);
		List<URL> urls = new ArrayList<URL>();
		for (File jarFile : fetchJarFiles(ketlHome)) {
			urls.add(jarFile.toURL());
		}
		addJarFilesToClassPath((urls.toArray(new URL[urls.size()])));

	}

	/**
	 * 
	 */
	private static void testAClass() {
		final String classNameWithPath = "net.sf.ehcache.util.PropertyUtil";
		try {
			Class otherClass = Class.forName(classNameWithPath);
			logger.fine(otherClass.getName() + " found.");
		} catch (ClassNotFoundException e) {
			logger.severe(e.getMessage());
		}
	}

	private static File[] fetchJarFiles(File file) {
		File[] jarFiles = file.listFiles(new FilenameFilter() {
			public boolean accept(File arg0, String name) {
				if (name.endsWith(".jar") && !name.equals("KETL.jar"))
					return true;
				else
					return false;
			}

		});
		return jarFiles;
	}

}
