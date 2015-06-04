'''
Created on Oct 12, 2014

@author: mingchen7
'''
import sys
import os
import re
from smtplib import SMTP_SSL as SMTP       # this invokes the secure SMTP protocol (port 465, uses SSL)
# from smtplib import SMTP                  # use this for standard SMTP protocol   (port 25, no encryption)
from email.MIMEText import MIMEText

SMTPserver = 'smtp.gmail.com'
sender = 'matthewchen719@gmail.com'
receivers = ['mingchen7@email.arizona.edu','jeff.cj.chen@gmail.com']

# typical values for text_subtype are plain, html, xml
text_subtype = 'plain'

content="""\
hahahahaha
"""

subject="Sent from Python"

USERNAME = "matthewchen719@gmail.com"
PASSWORD = "19890719"

try:
    msg = MIMEText(content, text_subtype)
    msg['Subject']= subject
    msg['From'] = sender # some SMTP servers will do this automatically, not all

    conn = SMTP(SMTPserver)
    conn.set_debuglevel(False)
    conn.login(USERNAME, PASSWORD)
    try:
        conn.sendmail(sender, receivers, msg.as_string())
    finally:
        conn.close()

except Exception, exc:
    sys.exit( "mail failed; %s" % str(exc) ) # give a error message
    
print "Email Sent" 