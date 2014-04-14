package com.kni.util.tableau;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.rmi.registry.Registry;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.RSAPublicKeySpec;
import java.util.ArrayList;
import java.util.List;

import javax.crypto.Cipher;
import javax.net.ssl.*;

import org.apache.commons.codec.binary.Hex;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.HTTP;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.kni.etl.ketl.exceptions.KETLException;
import com.kni.etl.util.XMLHelper;

@SuppressWarnings("deprecation")
public class ServerConnector {
	
	public class TableauResponse {

		private Document doc;
		private StatusLine status;

		/**
		 * Fetches The httpresp HttpResponse into a StringBuffer
		 * 
		 * @param httpresp
		 *            HttpResponse
		 * @return StringBuffer with contents of the HttpResponse
		 * @throws Exception
		 */
		public TableauResponse(HttpResponse httpresp)
				throws Exception {
			
			this.status = httpresp.getStatusLine();
			StringBuffer strbuffer = new StringBuffer();
			
			try {
				BufferedReader bufferedReader = new BufferedReader(
						new InputStreamReader(httpresp.getEntity().getContent()));

				String currentline = "";
				while ((currentline = bufferedReader.readLine()) != null) {
					strbuffer.append(currentline);
				}

				this.doc =  XMLHelper.readXMLFromString(strbuffer.toString());
			} catch (Exception e) {
				this.doc = null;
			}
			
			if (!this.success())
				throw new KETLException(this.status.getReasonPhrase() + ": " +  strbuffer.toString());
		}

		public boolean success(){
			return ! (this.status.getStatusCode() / 100 != 2);
		}
		public int exitCode() {
			return this.status.getStatusCode();
		}

		public String errorMessage() {
			return this.status.getReasonPhrase();
		}

		public String message() {
			return this.status.toString();
		}
	}
	

	
	private HttpClient client;
	private String authencity_token;
	private String serverAddress;

	
	/**
	 * Creates a HttpClient with authenticated connection to the Tableau Server
	 * 
	 * @param serveraddress
	 *            - URL address of the Tableau Server
	 * @param user
	 *            - Username on Tableau Server
	 * @param password
	 *            - Corresponding password to the Username
	 * @return HttpClient with authenticated connection to the Tableau Server
	 * @throws Exception
	 */
	public void authenticate(String serveraddress, String user, String password)
			throws Exception {
		// Initialize apache HttpClient
/*
		SSLSocketFactory sf = new SSLSocketFactory(sslContext, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
		*/
		HttpClient client = new DefaultHttpClient();
	   
	    //client.getConnectionManager().getSchemeRegistry().register(new Scheme("https", 443, sslsf)); 
		// Create Http Get request for authentication informations
		this.serverAddress = serveraddress;
		String url = serveraddress + "/auth.xml";

		HttpGet request = new HttpGet(url);
		HttpResponse response = client.execute(request);

		
		TableauResponse tResponse = new TableauResponse(response);

		if (!tResponse.success()){
			throw new Exception(tResponse.errorMessage());
		}
		
		// Get Required data for creating the authentication request, such as
		// modulus and exponent of the RSA public key and the authencity_token
		String modulusstr = null;
		String exponentstr = null;
		String authencity_token = null;

		NodeList elements = tResponse.doc.getElementsByTagName("authinfo");
		for (int i = 0; i < elements.getLength(); i++) {
			NodeList moduluses = ((Element) elements.item(i))
					.getElementsByTagName("modulus");
			for (int k = 0; k < moduluses.getLength(); k++) {
				modulusstr = moduluses.item(k).getTextContent();
			}
			NodeList exponents = ((Element) elements.item(i))
					.getElementsByTagName("exponent");
			for (int k = 0; k < exponents.getLength(); k++) {
				exponentstr = exponents.item(k).getTextContent();
			}
			NodeList authencity_tokens = ((Element) elements.item(i))
					.getElementsByTagName("authenticity_token");
			for (int k = 0; k < authencity_tokens.getLength(); k++) {
				authencity_token = authencity_tokens.item(k).getTextContent();
			}
		}

		// Parse the modulus and exponent into a BigInteger and create an RSA
		// public key from it
		BigInteger modulus = new BigInteger(modulusstr, 16);
		BigInteger exponent = new BigInteger(exponentstr, 16);

		KeyFactory keyFactory = KeyFactory.getInstance("RSA");
		RSAPublicKeySpec pub = new RSAPublicKeySpec(modulus, exponent);
		PublicKey pubkey = keyFactory.generatePublic(pub);

		Cipher cipher = Cipher.getInstance("RSA");
		cipher.init(Cipher.ENCRYPT_MODE, pubkey);

		// Encrypt the password with the created public key
		byte[] cipherData = cipher.doFinal(password.getBytes());
		String cryptedpass = new String(Hex.encodeHex(cipherData));

		// Create a post request for the authentication
		HttpPost postrequest = new HttpPost(serveraddress + "/auth/login.xml");

		MultipartEntityBuilder builder = MultipartEntityBuilder.create();
		builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
		builder.addPart("authenticity_token", new StringBody(authencity_token, ContentType.TEXT_PLAIN));
		builder.addPart("crypted", new StringBody(cryptedpass,
				ContentType.TEXT_PLAIN));
		builder.addPart("username", new StringBody(user, ContentType.TEXT_PLAIN));
		
		postrequest.setEntity(builder.build());
		
		HttpResponse postResponse = client.execute(postrequest);

		// We clear the entity here so we don't have to shutdown the client
		tResponse  = new TableauResponse(postResponse);
		
		if (!tResponse.success()){
			throw new Exception(tResponse.errorMessage());
		}
		
		elements = tResponse.doc.getElementsByTagName("authenticity_token");
		for (int i = 0; i < elements.getLength(); i++) {
			this.authencity_token = elements.item(i).getTextContent();
		}

		this.client = client;

	}

	public enum Type {
		workbook, datasource
	};

	public TableauResponse refreshExtract(String project, Type type, String name,
			boolean synchronous) throws Exception {
		HttpPost postRequest = new HttpPost(this.serverAddress
				+ "/refresh_extracts/" + type.name() + "s");

		MultipartEntityBuilder builder = MultipartEntityBuilder.create();
		builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
		builder.addPart("name", new StringBody(name, ContentType.TEXT_PLAIN));
		builder.addPart("project", new StringBody(project,
				ContentType.TEXT_PLAIN));
		builder.addPart("format", new StringBody("xml", ContentType.TEXT_PLAIN));
		if (synchronous)
			builder.addPart("synchronous", new StringBody("true",
					ContentType.TEXT_PLAIN));

		builder.addPart("authenticity_token", new StringBody(
				this.authencity_token, ContentType.TEXT_PLAIN));
		postRequest.setEntity(builder.build());
		postRequest.setHeader("User-Agent", "Tabcmd");
		HttpResponse response = this.client.execute(postRequest);
		return new TableauResponse(response);
	}

}