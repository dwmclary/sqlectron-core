import google from 'googleapis';

export default function getGoogleOAuth() {
  // This method looks for the GCLOUD_PROJECT and GOOGLE_APPLICATION_CREDENTIALS
  // environment variables.
  const googleAuthClient = google.auth.getApplicationDefault((err, authClient) => {
    if (err) {
      console.log('Authentication failed because of ', err);
      return;
    }
    if (authClient.createScopedRequired && authClient.createScopedRequired()) {
      const scopes = ['https://www.googleapis.com/auth/cloud-platform'];
      return authClient.createScoped(scopes);
    }
  });
  return googleAuthClient;
}
