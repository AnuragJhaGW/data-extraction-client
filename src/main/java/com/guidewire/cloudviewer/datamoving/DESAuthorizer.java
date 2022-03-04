package com.guidewire.cloudviewer.datamoving;

import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.builder.api.DefaultApi20;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.oauth.OAuth20Service;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class DESAuthorizer {

    private static final class InstanceHolder {
        private static final DESAuthorizer INSTANCE = new DESAuthorizer();
    }

    public static DESAuthorizer instance() {
        return InstanceHolder.INSTANCE;
    }

    /**
     * @param scopes a string of scopes delimited by a space character, for example
     *               "openid email tenant_id GwLiveSpotlight.singletenant.admin"
     */
    public static OAuth2AccessToken getAccessTokenFromOktaHub(String scopes, String oktaClientId, String oktaClientSecret, String oktaHost) throws InterruptedException, ExecutionException, IOException {
        OAuth20Service service = new ServiceBuilder(oktaClientId)
                .apiSecret(oktaClientSecret)
                .defaultScope(scopes)
                .build(new GWOkta20(oktaHost));
        return service.getAccessTokenClientCredentialsGrant();
    }

    static class GWOkta20 extends DefaultApi20 {
        private final String oktaHost;

        public  GWOkta20(String oktaHost){
            super();
            this.oktaHost = oktaHost;
        }
        @Override
        public String getAccessTokenEndpoint() {
//            return "https://guidewire-hub.oktapreview.com/oauth2/ausj9ftnbxOqfGU4U0h7/v1/token";
            return this.oktaHost+"/v1/token";
        }

        @Override
        protected String getAuthorizationBaseUrl() {
//            return "https://guidewire-hub.oktapreview.com/oauth2/ausj9ftnbxOqfGU4U0h7";
            return this.oktaHost;
        }
    }
}