import os
import io
import logging
import smtplib
import email
import imaplib
import datetime
import base64
import zipfile

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase

from config.file_config import FILETYPE
from utils.common.general_utils import extract_extension_from_filename
from utils.common.file_read_write import write_to_file


class EmailUtils:
    """ EMAIL UTILS class"""

    def __init__(self, email_config, mailbox='inbox'):
        try:
            """
            INPUT:
                host - smtp host
                user_name - email username
                password - email password
                port - smtp port
                mailbox - mail box
            """
            self.host = email_config.get('EmailSMTPHost')
            self.user_name = email_config.get('EmailUser')
            self.password = email_config.get('EmailPassword')
            self.port = email_config.get('EmailSMTPport')
            self.email_imap_host = email_config.get('EmailIMAPHost')

            if self.email_imap_host:
                logging.info("EmailIMAPHost provided for login")
                self.mailIn = imaplib.IMAP4_SSL(self.email_imap_host)
                self.mailIn.login(self.user_name, self.password)
                self.mailIn.select(mailbox)

            logging.info("Connecting to SMTP for outgoing mail")
            self.mail_out = smtplib.SMTP(self.host, self.port)
            self.mail_out.ehlo()
            self.mail_out.starttls()
            self.mail_out.ehlo
            self.mail_out.login(self.user_name, self.password)
            logging.info("login done")
        except Exception as e:
            logging.info("Error while creating the connection")
            raise Exception(e)

    def send(self, comma_seperated_receipents, msg_subject, msg_body, msg_attachments=[]):
        """ Send email function
        INPUT:
            msg_from - email from address
            comma_seperated_receipents - email receipents seperated by comma
            msg_subject - email message subject
            msg_body - email message body
            msg_attachments - attachements to send in mail
        OUTPUT:
            send email
        """
        try:
            logging.info("sending email....")
            msg = MIMEMultipart()
            msg['From'] = self.user_name
            msg['To'] = comma_seperated_receipents
            msg['Subject'] = msg_subject
            msg.attach(MIMEText(msg_body))

            for each_attachment in msg_attachments:
                part = MIMEBase('application', "octet-stream")
                with open(each_attachment, "rb") as file:
                    part.set_payload(file.read())
                # After the file is closed
                email.encoders.encode_base64(part)
                part.add_header('Content-Disposition', 'attachment; filename="{}"'.format(os.path.basename(each_attachment)))
                msg.attach(part)
            if ',' in comma_seperated_receipents:
                to_list = comma_seperated_receipents.split(',')
            elif ';' in comma_seperated_receipents:
                to_list = comma_seperated_receipents.split(';')
            else:
                to_list = [comma_seperated_receipents]
            self.mail_out.sendmail(self.user_name, to_list, msg.as_string())
            logging.info('Email sent...')
        except Exception as e:
            logging.info("Error while sending the email")
            raise Exception(e)

    def search(self, msg_from=None, msg_subject=None, msg_date=datetime.date.today()):
        """ Search email function
        INPUT:
            msg_from - email from address
            msg_subject - email message subject
            msg_date - date to search for email
        OUTPUT:
            search email and returns result
        """
        try:
            if msg_from is not None:
                s = "(FROM \"{}\") ".format(msg_from)
            if msg_subject is not None:
                s = s + "(SUBJECT \"{}\") ".format(msg_subject)
            if msg_date is not None:
                if isinstance(msg_date, datetime.date):
                    s = s + "(SINCE \"{}\") ".format(msg_date.strftime('%d-%b-%Y'))
                else:
                    s = s + "(SINCE \"{}\") ".format(msg_date)
            result, data = self.mailIn.search(None, s.strip())
            logging.info("Search results {}".format(len(data[0].split())))
            results = []

            for num in data[0].split():
                result, data = self.mailIn.fetch(num, "(RFC822)")

                raw_email = data[0][1]
                # here's the body, which is raw text of the whole email
                try:
                    raw_email_string = raw_email.decode('utf-8')
                except Exception as e:
                    logging.error("Raw email string value = " + str(raw_email))
                    logging.error("Error occured while reading email data =" + str(e))
                    continue

                email_message = email.message_from_string(raw_email_string)

                msg_object = {'from': email_message['from'], 'to': email_message['to'], 'subject': email_message['subject'],
                              'date': email_message['date'], 'attachments': []}
                for part in email_message.walk():
                    cd = part.get('Content-Disposition')
                    if cd is not None:
                        if 'attachment' in cd.lower():
                            # append filename in attachment
                            msg_object['filename'] = part.get_filename()
                            # append attachment to search result
                            msg_object['attachment'] = part.get_payload()
                    ct = part.get('Content-Type')
                    if ct is not None:
                        if 'text/html' in ct:
                            msg_object['html'] = part.get_payload(decode=True)
                            continue
                        if 'text/plain' in ct:
                            msg_object['text'] = part.get_payload(decode=True)
                            continue
                    # Unknown message part
                    logging.info("Unknow message part:")
                # Append result
                results.append(msg_object)

            return results
        except Exception as e:
            logging.error("Error while searching the email")
            raise Exception(e)

    def search_and_download(self, msg_from=None, msg_subject=None, msg_date=datetime.date.today(), local_path_to_save_file=None, file_type_to_save_after_extraction=FILETYPE.CSV_FILE_TYPE):
        """ Function to search and download the attachment in the email
            INPUT:
                msg_from - email from address
                msg_subject - email message subject
                msg_date - date to search for email
                local_path_to_save_file - Full path to save attachment file
                file_type_to_save_after_extraction - output file type after extracting zip
            OUTPUT:
                search and download the attachment into loacl path which is specified in input
                returns result_to_return - {dict}
                    {
                        output_file_name - filename which is downloaded in loacl directory
                    }
        """
        try:
            search_result = self.search(msg_from=msg_from, msg_subject=msg_subject, msg_date=msg_date)
            result_to_return = {}
            if len(search_result) is 0:
                logging.info("Search result is 0")
                raise Exception("No search results found")
            file_name_with_extension = search_result[0]['filename']
            file_name, file_extension = extract_extension_from_filename(file_name_with_extension)
            mail_attachment = search_result[0].get('attachment')
            if file_extension == FILETYPE.ZIP_FILE_TYPE:
                logging.info("Extracting zip file and saving to local directory")
                # Add Output file name with extension to result
                result_to_return['output_file_name'] = file_name + file_type_to_save_after_extraction
                # Save output file name in result
                zip_bytes = base64.b64decode(mail_attachment)
                # wraps up the bytes into a file
                file_wrapper = io.BytesIO(zip_bytes)
                # unzip the file to local_dir
                if zipfile.is_zipfile(file_wrapper):
                    with zipfile.ZipFile(file_wrapper, 'r') as zf:
                        zf.extractall(local_path_to_save_file)
            elif file_extension == FILETYPE.CSV_FILE_TYPE:
                logging.info("Extracting csv file and saving to local directory")
                result_to_return['output_file_name'] = file_name_with_extension
                csv_bytes = base64.b64decode(mail_attachment)
                write_to_file(local_path_to_save_file + file_name_with_extension, csv_bytes)
                logging.info("csv file downloaded")
            else:
                logging.error("Unknown File type")
                raise Exception("Unknown FIle type")
            return result_to_return
        except Exception as e:
            logging.error("Error in searching email and downloading attachment")
            raise e

    def get_email_body_text_from_search_result(self, search_result_list):
        """Find and return email body text from email search result list
            INPUT:
                search_result_list - email search result list
            OUTPUT:
                return email body text in string format
        """
        try:
            # get email body
            email_body_text = search_result_list[0].get('text')
            # decode bytes to string
            email_body_text = email_body_text.decode("utf-8")
            return email_body_text
        except Exception as e:
            logging.error("Error while getting email body text")
            raise Exception(e)
