
import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.apache.ws.security.WSPasswordCallback;

/**
 * PWCallback for the Server
 * Dao's notes: this class is used to check the password.  TODO: check the password!
 */
public class PWCallback implements CallbackHandler {
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {

        for (int i = 0; i < callbacks.length; i++) {
            if (callbacks[i] instanceof WSPasswordCallback) {
                WSPasswordCallback pc = (WSPasswordCallback)callbacks[i];
                
                // set the password given a username
                if ("wss4j".equals(pc.getIdentifer())) {
                    pc.setPassword("123123");
                }
                
                // NOTE: again we can only validate via the userid variable right now. 
/*                if (hashedStr.equals(pc.getIdentifer())) {
                      pc.setPassword("anything_here");
                } else {
                      // doing this actually shows up as "Callback supplied no password for: wss4j" for .NET
                      throw new UnsupportedCallbackException(callbacks[i], "Unrecognized Callback");
                }
*/                 
                    
            } else {
                throw new UnsupportedCallbackException(callbacks[i], "Unrecognized Callback");
            }
        }
    }
}