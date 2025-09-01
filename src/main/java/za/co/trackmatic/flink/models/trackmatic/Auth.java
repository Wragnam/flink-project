package za.co.trackmatic.flink.models.trackmatic;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Represents authentication response containing token data.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Auth implements Serializable {

    /**
     * Represents the actual token string.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Token implements Serializable{

        @JsonProperty("token")
        private String token;

        public Token() {

        }

        public String getToken() {
            return token;
        }

        public void setToken(String token) {
            this.token = token;
        }
    }

    /**
     * Wrapper class for token data returned from API key authentication.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TokenWrapper implements Serializable{

        private Token getTokenFromApiKey;

        public TokenWrapper() {}

        public Token getGetTokenFromApiKey() {
            return getTokenFromApiKey;
        }

        public void setGetTokenFromApiKey(Token getTokenFromApiKey) {
            this.getTokenFromApiKey = getTokenFromApiKey;
        }
    }

    /** Wrapper data containing token information */
    private TokenWrapper data;

    public Auth() {

    }

    public TokenWrapper getData() {
        return data;
    }

    public void setData(TokenWrapper data) {
        this.data = data;
    }
}
