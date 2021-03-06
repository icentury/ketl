package com.kni.util.tableau;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.RSAPublicKeySpec;

import javax.crypto.Cipher;

import org.apache.commons.codec.binary.Hex;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.SystemDefaultHttpClient;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.util.XMLHelper;

@SuppressWarnings("deprecation")
public class ServerConnector {

  public class TableauResponse {

    private Document doc;
    private StatusLine status;

    /**
     * Fetches The httpresp HttpResponse into a StringBuffer
     * 
     * @param httpresp HttpResponse
     * @return StringBuffer with contents of the HttpResponse
     * @throws Exception
     */
    public TableauResponse(HttpResponse httpresp) throws Exception {

      this.status = httpresp.getStatusLine();
      StringBuffer strbuffer = new StringBuffer();

      try {
        BufferedReader bufferedReader =
            new BufferedReader(new InputStreamReader(httpresp.getEntity().getContent()));

        String currentline = "";
        while ((currentline = bufferedReader.readLine()) != null) {
          strbuffer.append(currentline);
        }

        this.doc = XMLHelper.readXMLFromString(strbuffer.toString());
      } catch (Exception e) {
        this.doc = null;
      }

      if (!this.success())
        throw new KETLThreadException(this.status.getReasonPhrase() + ": " + strbuffer.toString(),
            this);
    }

    public boolean success() {
      return !(this.status.getStatusCode() / 100 != 2);
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



  private String authencity_token;
  private String serverAddress;
  private SystemDefaultHttpClient client;
  private String cryptedpass;
  private String user;


  /**
   * Creates a HttpClient with authenticated connection to the Tableau Server
   * 
   * @param serveraddress - URL address of the Tableau Server
   * @param user - Username on Tableau Server
   * @param password - Corresponding password to the Username
   * @return HttpClient with authenticated connection to the Tableau Server
   * @throws Exception
   */
  public void authenticate(String serveraddress, String user, String password) throws Exception {
    // Initialize apache HttpClient
    /*
     * SSLSocketFactory sf = new SSLSocketFactory(sslContext,
     * SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
     */
    this.user = user;
    client = new SystemDefaultHttpClient();
    // Create Http Get request for authentication informations
    this.serverAddress = serveraddress;
    String url = serveraddress + "/auth.xml";

    HttpGet request = new HttpGet(url);
    HttpResponse response = client.execute(request);


    TableauResponse tResponse = new TableauResponse(response);

    if (!tResponse.success()) {
      throw new Exception(tResponse.errorMessage());
    }

    // Get Required data for creating the authentication request, such as
    // modulus and exponent of the RSA public key and the authencity_token
    String modulusstr = null;
    String exponentstr = null;
    String authencity_token = null;

    NodeList elements = tResponse.doc.getElementsByTagName("authinfo");
    for (int i = 0; i < elements.getLength(); i++) {
      NodeList moduluses = ((Element) elements.item(i)).getElementsByTagName("modulus");
      for (int k = 0; k < moduluses.getLength(); k++) {
        modulusstr = moduluses.item(k).getTextContent();
      }
      NodeList exponents = ((Element) elements.item(i)).getElementsByTagName("exponent");
      for (int k = 0; k < exponents.getLength(); k++) {
        exponentstr = exponents.item(k).getTextContent();
      }
      NodeList authencity_tokens =
          ((Element) elements.item(i)).getElementsByTagName("authenticity_token");
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
    this.cryptedpass = new String(Hex.encodeHex(cipherData));

    // Create a post request for the authentication
    HttpPost postrequest = new HttpPost(serveraddress + "/auth/login.xml");

    MultipartEntityBuilder builder = MultipartEntityBuilder.create();
    builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
    builder.addPart("authenticity_token", new StringBody(authencity_token, ContentType.TEXT_PLAIN));
    builder.addPart("crypted", new StringBody(cryptedpass, ContentType.TEXT_PLAIN));
    builder.addPart("username", new StringBody(user, ContentType.TEXT_PLAIN));

    postrequest.setEntity(builder.build());

    HttpResponse postResponse = client.execute(postrequest);

    // We clear the entity here so we don't have to shutdown the client
    tResponse = new TableauResponse(postResponse);

    if (!tResponse.success()) {
      throw new Exception(tResponse.errorMessage());
    }

    elements = tResponse.doc.getElementsByTagName("authenticity_token");
    for (int i = 0; i < elements.getLength(); i++) {
      this.authencity_token = elements.item(i).getTextContent();
    }


  }

  public enum Type {
    workbook, datasource
  };

  public enum Format {
    pdf, csv, png, fullpdf
  };

  public enum PageLayout {
    portrait, landscape
  }

  public enum PageSize {
    letter, unspecified, legal, note, folio, tabloid, ledger, statement, executive, a3, a4, a5, b4,
    b5, quarto;
  }

  public TableauResponse refreshExtract(String project, Type type, String name, boolean synchronous)
      throws Exception {
    HttpPost postRequest =
        new HttpPost(this.serverAddress + "/refresh_extracts/" + type.name() + "s");

    MultipartEntityBuilder builder = MultipartEntityBuilder.create();
    builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
    builder.addPart("name", new StringBody(name, ContentType.TEXT_PLAIN));
    builder.addPart("project", new StringBody(project, ContentType.TEXT_PLAIN));
    builder.addPart("format", new StringBody("xml", ContentType.TEXT_PLAIN));
    if (synchronous)
      builder.addPart("synchronous", new StringBody("true", ContentType.TEXT_PLAIN));

    builder.addPart("authenticity_token", new StringBody(this.authencity_token,
        ContentType.TEXT_PLAIN));
    postRequest.setEntity(builder.build());
    postRequest.setHeader("User-Agent", "Tabcmd");
    HttpResponse response = this.client.execute(postRequest);
    return new TableauResponse(response);
  }



  class ExportConfig {
    public ExportConfig(String workbook, String view) {
      this.workbook = workbook;
      this.view = view;
    }

    PageSize pagesize = PageSize.letter;
    int width = 800;
    int height = 600;
    Format format = Format.pdf;
    PageLayout pagelayout = null;

    final String workbook, view;
  }

  public ExportConfig getExportConfig(String workbook, String view) {
    return new ExportConfig(workbook, view);
  }

  public TableauResponse export(ExportConfig config) throws Exception {
    HttpGet postRequest =
        new HttpGet(this.serverAddress + "/views" + File.separator + config.workbook
            + File.separator + config.view);

    MultipartEntityBuilder builder = MultipartEntityBuilder.create();
    builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
    builder.addPart("authenticity_token", new StringBody(this.authencity_token,
        ContentType.TEXT_PLAIN));
    // postRequest.setEntity(builder.build());
    postRequest.setHeader("User-Agent", "Tabcmd");
    HttpResponse response = this.client.execute(postRequest);
    return new TableauResponse(response);
  }

}
