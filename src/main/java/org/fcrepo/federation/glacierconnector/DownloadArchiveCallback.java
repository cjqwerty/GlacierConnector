package org.fcrepo.federation.glacierconnector;

import java.util.Properties;

//import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class DownloadArchiveCallback implements CallbackInterface {

	private String username = "noreply@gmail.com";
	private String password = "password";
	private String to = "to@gmail.com";

	private String subject = "A glacier archive is downloaded.";

	Pool jobPool;

	public String GetUserEmailAddressById(String achitiveId) {
		// TODO
		return to;
	}

	DownloadArchiveCallback(String sendFrom, String password, String to) {
		this.username = sendFrom;
		this.password = password;
		this.to = to;

	}


	private String buildText(String architiveId) {
		return "ArchiveId: " + architiveId + "\nThis notice comes from the callback interface.";
	}

	private void SendNoticeEmail(String architiveId) {

		System.out.println("Sending an email...");
		
		Properties props = new Properties();
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.port", "587");

		Session session = Session.getInstance(props,
				new javax.mail.Authenticator() {
					protected PasswordAuthentication getPasswordAuthentication() {
						return new PasswordAuthentication(username, password);
					}
				});

		try {

			MimeMessage message = new MimeMessage(session);
			message.setFrom(new InternetAddress(username));
			message.setRecipients(MimeMessage.RecipientType.TO,
					InternetAddress.parse(to));
			message.setSubject(subject);
			message.setText(buildText(architiveId));

			Transport.send(message);

			System.out.println("Email is sent.");

		} catch (MessagingException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void CallBack(String archiveId) {
		System.out.println("Enter returnResult ...");

		GlacierMessageListener.unregister(archiveId);
		
		SendNoticeEmail(archiveId);

		System.out.println("leave returnResult ");

	}

}
